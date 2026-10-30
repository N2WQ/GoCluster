---
title: Commands And HELP
description: Command ownership and compiled HELP contracts for GoCluster.
---

This directory is the source of truth for telnet command help. The public `HELP` output is built in [`processor.go`](processor.go), not copied from a separate static document.

This guidance describes the current checkout's independent GO Boolean and
native CC filter banks. It does not establish a production deployment.

## What Lives Here

- command parsing for `HELP`, `DX`, `SHOW`, `BYE`, and related aliases
- per-dialect HELP catalogs and task-oriented overviews
- build, dedupe, and path-glyph HELP notes injected from runtime snapshots
- history, DXCC, propagation-outlook, and build-info read paths used by
  `SHOW DX`, `SHOW MYDX`, `SHOW DXCC`, `SHOW PROP`, and `SHOW BUILD`
- own-call identity display used by `SHOW OWN`

## HELP Source Of Truth

HELP rendering and its detailed references are owned by:

- `helpOverviewLines(...)` in [`help_overview.go`](help_overview.go)
- `buildHelpCatalog(...)`
- `filterReferenceHelpLines(...)` in [`help_overview.go`](help_overview.go)
- `BooleanRuleHelpLines()` in `commands/boolean_rule_help.go`, installed by
  `installBooleanRuleHelp(...)` in `commands/boolean_rule_catalog.go`
- `pathGlyphHelpLines(...)`
- `dedupeHelpNotes(...)`
- `installConfigurationHelp(...)` in [`configuration_help.go`](configuration_help.go)
- `installCommentHelp(...)` in [`comment_help.go`](comment_help.go)
- catalog head/alias routing in [`help_routing.go`](help_routing.go)

The overview groups everyday commands by task. In GO, `HELP RULE` owns the
Boolean expression, field, vocabulary, default/reset and bounds reference.
PASS rules are ORed and matching REJECT vetoes them. In CC, `HELP SET/FILTER`
owns native geography, frequency-slot and switch guidance; `HELP FILTERS`
explains their composition. CC refuses Boolean mutations with native guidance.

`buildHelpCatalog(...)` in `processor.go` installs configuration HELP, COMMENT
HELP and then Boolean rule HELP before routing aliases. The registered
`BooleanRuleHelpLines` provider owns expression wording, not the unregistered
telnet `formatBooleanRuleHelp` helper. `configuration_help.go` owns human
readback and machine command topics; execution remains under telnet.

GO DXDXCC/DEDXCC operands and CC DOC/DXCTY lists accept exact canonical CTY
labels and positive ADIF numbers in human commands and YAML requests. Labels
resolve once per request; accepted configurations use numeric codes. HELP
distinguishes entity selection (`DXDXCC=IT9`, ADIF 248) from literal GO calls
(`DXCALL=IT9*`).

Separate controls retain their own grammar. Shared PASS/REJECT control forms
work in both dialects; CC SET/FILTER and UNSET/FILTER support BEACON, WWV, WCY,
ANNOUNCE/PC93, SELF and TOXIC as separate switches. Switches take no value or ALL.
Only SET/FILTER accepts `<switch>/ON|/OFF`, without extra values; UNSET has no
inline flags. SELF selects spots of the user's own call. Named CC control
shortcuts retain their no-argument grammar. Native CC also supports global
DOC/DOS/DXCTY/DXSTATE lists, operator-enabled mode shortcuts, SKIMMER selection,
DXBM and SET/NOFILTER. Per-band geography is available through schema-seven
YAML; its terminal grammar and CC User integration remain unverified. MINSNR,
COMMENT-list and Boolean mutations require GO expressions. Historical mechanics in
[TSR-0064](../docs/troubleshooting/TSR-0064-cc-help-category-grammar.md)
are not a compatibility contract.

`HELP SYMBOLS` contains the existing confidence legend and the path legend when
its configured display is available. Configured glyphs and reporting windows
still come from startup snapshots; help does not read external text files.

YAML client commands retain command-specific help, such as
`HELP GET YAML CONFIG`. Native CC guidance points to YAML for long replacing
lists and per-band geography.

HELP accepts a working command spelling with arguments, for example
`HELP SH DX 5`, `HELP SET SOLAR 30` or `HELP GET YAML CONFIG SCHEMA 6 ID read-1`.
It selects the longest complete command head without executing the command.
SH BUILD/OWN/DXCC/MYDX/PROP and history aliases resolve to their own topics;
shared PASS/REJECT topics also resolve in CC. Fixed CC shortcuts take priority
over loaded mode aliases. Unsupported heads remain explicit unknown topics;
HELP does not add executable aliases. See
[routing troubleshooting](../docs/troubleshooting/TSR-0065-help-routing-alias-coverage.md).

The main landing page [`../README.md`](../README.md) contains a generated HELP
block for the default `go` dialect, with a processor-output comparison test.
Regenerating that block is outside this documentation slice; it is not the
authority for the active bank's filter contract.

Reviewed GO and CC overview fixtures live under [`testdata`](testdata).
Tests also check command discoverability, reference navigation and line widths.
The emitted `filterRecipeLines` recipe begins with `RESET FILTER RULE ALL`;
it deliberately removes every personal slot before installing its combined
BAND/MODE rule. `telnet/help_recipe_test.go` instead exercises a combined
slot-0 replacement retaining an independent REJECT. That different sequence
does not prove the emitted reset recipe preserves unrelated rules. These
tests cover different operator sequences. The pre-v5 category mechanics in
[ADR-0264](../docs/decisions/ADR-0264-task-oriented-compiled-help.md) are historical.

## Dialects

The runtime supports two command dialects:

- `go`: the default dialect shown in the main README
- `cc`: native CC mode/source, geography and frequency-slot selections, plus
  `SHOW/DX`, `SET/FILTER`, `UNSET/FILTER` and `SET/ANN`-style control shortcuts

Dialect selection is user-visible and persisted per callsign. HELP is rendered for the active dialect, so the command list and examples change with the selected dialect.

Each dialect selects its own retained filter bank and six switches. Switching
dialects preserves the other bank; grid, noise, dedupe, path and solar settings
are shared. Bare PASS/REJECT in CC points to native help; numbered rules require
GO. PC93 remains an ANNOUNCE control alias. Historical discovery wording in
[TSR-0066](../docs/troubleshooting/TSR-0066-command-discovery-wording.md)
does not define the current mutation surface.

## Manual DX Admission

Comments accept ASCII letters, digits, spaces, and `/ # @ - . , * ? _`.
Other punctuation and non-ASCII characters are rejected before DX dispatch.
The node's command byte limit applies to the whole edited command, excluding
its line ending; the default is 128 bytes. Mode/report tokens may be parsed
from the comment. See `HELP DX` for both supported argument orders.

Plain decimal frequencies are in kHz. Before admission, the effective value is
rounded to the nearest 10 Hz (0.01 kHz), using the same rounding as Spot
construction, and checked for finiteness and the inclusive Spot bounds:
135.70 through 2450000.00 kHz. Values in gaps inside that envelope retain their
existing acceptance. An invalid value queues no spot and consumes no posting
allowance. Admission uses the rounded value's band. `HELP DX` exposes the
current envelope.

A manual `DX` post that enters the queue returns `Spot queued.`. When the cluster's
reputation gate refuses the post, it returns `Spot rejected.` and queues no
spot. That message deliberately omits probation, allowance and IP-history
details; configured operator reputation-drop diagnostics retain the cause.
Numeric SSIDs share the baseline station allowance while saved settings remain
separate. See the [telnet guide](../telnet/README.md) and
[reputation configuration](../data/config/reputation.yaml).

Posting requires a valid logged-in callsign. Logins ending TEST require CTY
metadata for their baseline callsign. An accepted queue item still undergoes
processing and recipient filtering; `Spot queued.` does not establish delivery.
Missing or full input queues return explicit refusals.

## Command Surface

The operator-facing commands handled here are:

- `HELP [topic]`
- `DX`
- `SHOW DX`
- `SHOW MYDX`
- `SHOW DXCC`
- `SHOW WWV [FULL]` and `SHOW WCY [FULL]`
- `SHOW PROP`
- `SHOW BUILD`
  shows the startup-resolved UTC `YYMMDD` version, release tag, and Go toolchain
  when available; unstamped release tags are omitted;
  commit and build timestamp remain separate metadata in
  `--version` and PC18, rather than appearing in the version string.
- `SHOW OWN`
- GO: `SHOW FILTER [RULE|FULL]`, `HELP RULE`, `PASS RULE <0..9> <expression>`,
  `REJECT RULE <0..9> <expression>`, `RESET FILTER RULE <0..9|ALL>`
- CC: `SHOW/FILTER [DOC|DOS|DXCTY|DXSTATE|DXBM|MODE|SOURCE|FULL]`,
  `SET/FILTER <category>/<PASS|REJECT> <list>`, `SET/FILTER <category>/OFF`,
  `SET/NOFILTER`, configured `SET/<mode>`, `SET/NO<mode>`, `UNSET/<mode>`,
  `SET/SKIMMER`, `SET/NOSKIMMER`, `UNSET/SKIMMER`
- `SHOW SETTINGS`
- `WHOSPOTSME [band]`
- `PAUSE [seconds]`
- `SHOW HOLD`
- `RESUME`
- `SHOW DEDUPE`
- `SET DEDUPE`
- `SET DIAG`
- `SET GRID`
- `SET NOISE`
- `SET PATHSAMPLES`
- `SET SOLAR`
- GO: `RESET FILTER`
- `SAVE PRESET <name>`
- `LIST PRESET`
- `LOAD PRESET <name>`
- `DELETE PRESET <name>`
- `DIALECT`
- `BYE`
- `GET YAML FILTER|SETTINGS|CONFIG|CAPABILITIES SCHEMA 6|7 [ID <id>]`
- `PUT YAML FILTER|SETTINGS|CONFIG [SCHEMA 7]`
- `PATCH YAML FILTER|SETTINGS|CONFIG [SCHEMA 7]`
- `VALIDATE YAML CONFIG [SCHEMA 7]`

Filter mutation, named presets, configuration readbacks/uploads, `SHOW PROP`
execution, and read-pause state are handled in the telnet layer.
This package documents those commands in HELP, but the parsers for `PASS`,
`REJECT`, `SHOW FILTER`, `SHOW SETTINGS`, GET/PUT/PATCH/VALIDATE YAML, `SHOW PROP`,
`PAUSE`, `SHOW HOLD`, `RESUME`, and the `cc` aliases live under [`../telnet`](../telnet).

In GO, setting a slot replaces its action and entire expression. Slots are not
priorities; another PASS broadens selection. No active PASS means unrestricted
personal selection except matching REJECTs. New accounts and old-profile resets
receive the same editable default PASS slot 0; restored Boolean empty rules stay empty.
A PASS rule without SOURCE allows HUMAN and SKIMMER.
SOURCE follows ordinary Boolean semantics, including OR and NOT.
Explicit SOURCE=HUMAN stays human-only, including saved default rules.
Shipped defaults select both sources.
RESET FILTER RULE clears selected slots; RESET FILTER clears all slots,
including any NEARBY terms. Neither reinstalls defaults nor changes
separate controls/settings. See [the full rule contract](../telnet/README.md#filters).

GO `SHOW FILTER` and `SHOW FILTER RULE` show current settings, exact numbered
canonical rules and individual controls.
`SHOW FILTER FULL` adds one-program preparation counts and control detail.
No view flattens Boolean alternatives into independent field lists.
`SHOW/FILTER` and `SH/FILTER` are aliases. In CC they show native criteria and
support category-specific views instead of RULE. The native readback distinguishes
band INHERIT from explicit OFF and DXBM OFF from active allow-all.
`GET YAML FILTER SCHEMA 6` exposes GO expressions and exact DEFAULT/boolean
controls when GO is active. Schema 7 FILTER describes the tagged active bank.
`SHOW SETTINGS`
separates configured preferences and effective behavior from session status.
`SHOW/SETTINGS` and `SH/SETTINGS` are equivalent readback aliases.
Path minimums reflect active runtime state for both stations and beacons.
Both views report the associated preset and whether preferences differ from
its retained reference snapshot.

Human lines contain at most 78 printable ASCII characters followed by CRLF.
Exact strings use Go-style quoted ASCII escapes; complete quoted pieces joined
by `+` preserve long values without trimming spaces or splitting escapes.
This is display encoding, not expression quoting or necessarily a copyable
command. Slots display in numeric order, not priority order. Human input
retains the configured limit (128 bytes by default); use YAML for longer
expressions or native lists. Rendering reads retained immutable prepared plans.

CC mode and source selections both apply; an empty list admits no spots.
DOC/DOS select the spotter country/state and DXCTY/DXSTATE select the DX.
Categories use AND and values within a list use OR. Missing or invalid metadata
fails PASS and survives REJECT. Global edits replace the action/list and clear
that category's per-band exceptions, including an otherwise equal replacement.
OFF removes only its category's restriction. Schema-seven band entries replace
the global category on that band; an absent entry inherits global and explicit
OFF is unrestricted. State codes use the existing registered-address metadata,
without an additional country check.

DXBM selects fixed frequency slots independently of the decoded mode. Its 15
bands contain 37 slots, including 4-MTR and MW-MW. The first PASS after OFF
starts a whitelist; the first REJECT starts a blacklist. Later edits change
only the named slots, and whole-band selections expand to their slots. The
higher slot owns a shared endpoint, with the final upper endpoint included.
OFF is different from an active allow-all selection. Native terminal mode
shortcuts require the operator taxonomy's cc_shortcut flag; mode edits apply to
both sources and source edits leave modes unchanged. SET/NOFILTER restores the
current operator mode/source defaults and clears geography/DXBM while keeping
six switches, the inactive GO bank and shared settings. OWN, BOB, DX/NODX,
TALK, WX and announcement/weather geography are unavailable.

These human readbacks always suppress live spots during preparation, queueing
and delivery, followed by a full reading interval after the server write and
flush. They use the positive configured duration or 30 seconds when it is
zero, ignoring the row threshold even when disabled. Longer existing pauses
are preserved; later valid PAUSE/RESUME controls supersede earlier pending
completion effects. Human size errors follow the same pause policy.

Client YAML commands never change pause state,
including on success or error. Each reply is one complete framed control
message with no human footer. GET exposes exact writable configuration
separately from read-only status. Every GET requires explicit SCHEMA 6 or
SCHEMA 7. Schema 6 FILTER/CONFIG is available only while GO is active; schema
6 SETTINGS and CAPABILITIES work in both dialects. A schema 6 CONFIG write
must remain GO. SETTINGS may switch the selected bank. Schema 7 CONFIG is the
complete backup of both banks, both sets of switches and shared settings;
schema 6 is a legacy view, not a complete backup. CAPABILITIES advertises
supported schemas, fields, vocabulary and bounds. Optional GET IDs preserve
case and accept 1-32 ASCII letters, digits or hyphens.

The legacy schema 6 FILTER has seven fields: `rules`, `include_beacons`,
`allow_wwv`, `allow_wcy`, `allow_announce`, `allow_self`, `allow_toxic`.
SETTINGS retains its six fields: `dialect`, `grid`, `noise_class`,
`dedupe_policy`, `path_min_observation_count`, `solar_summary_minutes`.
Schema 6 CONFIG nests `filters` and `settings`. Each rule requires integer slot 0–9,
lowercase action pass/reject and a complete `expr`.

PUT requires the complete resource. Schema 7 PUT CONFIG explicitly restores
both banks and shared settings. VALIDATE accepts complete CONFIG only, without
applying or saving. Schema 7 FILTER is tagged for the active dialect. Ordinary
PATCH edits the active bank/shared settings and refuses an inactive-bank edit
or a filter edit combined with a dialect switch. Omitted fields remain; a
supplied GO rules list replaces all slots. A supplied CC global geography row
replaces its list and clears that category's band exceptions. A bands-only
PATCH changes named rows; action INHERIT removes one override. Global and band
edits cannot be combined for one category in a PATCH. A supplied DXBM object
replaces its complete active/base/slot state. Preserve DEFAULT, explicit booleans,
empty selections and zero sentinels, subject to admission.

Legacy three-token write headers select schema 6 and its 65,536-byte body
limit. Schema 7 writes require an explicit SCHEMA 7 header and matching body,
for example `PUT YAML CONFIG SCHEMA 7`; their body limit is 262,144 bytes.
The strict envelope permits
`schema_version`, `request_id`, `if_revision` and `configuration`; bodies require
the selected `schema_version`, request_id and configuration. PUT/PATCH also
require the revision from GET as if_revision; VALIDATE permits it optionally.
GET again after reconnect or a conflict. Validation or persistence failure
leaves live and saved configuration
unchanged; unavailable choices are rejected. Even an unchanged PUT persists
before success without resetting unrelated runtime state. Writes preserve the
preset reference and can set or clear modified. Unknown/read-only fields,
duplicate keys/slots, aliases, anchors,
merges, custom tags, nulls and extra documents are refused without translation.

GO bounds are 10 slots, 1,024 raw and canonical expression bytes, 32 atoms/128
nodes/depth 8 per rule, 32 values per atom and 128 atoms/512 values total.
COMMENT operands are 1–64 printable ASCII bytes with a non-space byte.
Parentheses/NOT count toward nesting, not flat AND/OR height. Machine,
human-line, storage and decode-work bounds are separate. CC permits 128 modes,
two sources, 512 country values or 73 state values per applicable category
row, four global geography rows and at most 15 band rows per category. Canonical
CC is limited to 128 KiB, complete schema-seven CONFIG/tagged-preset content to
192 KiB and framed machine responses to 256 KiB. Acceptance also checks the
complete profile, both retained preset baselines, preset/library storage and
decode-work bounds, so an accepted configuration can be exported and restored.

Legacy responses retain their 65,536-byte limit. Native CC human readbacks and
schema-seven machine frames have a 262,144-byte limit, including CRLF, framing
and any human footer, with complete output or an explicit error. Upload body
limits exclude markers and count actual LF/CRLF bytes. Uploads retain the
30-second deadline from valid-header acceptance. Large schema-seven transfers
have separate process-wide 8 MiB inbound and outbound reservations; one client
may own one large output at a time. Busy admission fails without waiting.
Malformed upload headers, oversize, expiry and unreliable/incomplete framing
close the connection without dispatching payload remnants. A fully received
invalid document gets a YAML error and keeps the connection open. Ordinary
command headers keep their configured limit (128 bytes in the shipped config).

See the [human display contract](../telnet/README.md#human-configuration-readbacks)
and [client YAML configuration](../telnet/README.md#client-yaml-configuration).

`PAUSE` defaults to 30 seconds and accepts whole seconds from 1 to 300 in both
dialects. It remains available when automatic read pause is disabled. A new
`PAUSE` sets a fresh duration; automatic pauses may extend an active pause but
cannot shorten it. `SHOW HOLD` reports either pause, and `RESUME` ends it without
replaying missed spots. Command replies and other control traffic continue.

`SHOW HOLD` accepts exactly those two tokens, ignoring case and allowing
repeated ASCII spaces. Extra arguments return usage without changing pause
state. Its status and usage replies bypass automatic pausing. `SH HOLD` and
slash forms are unsupported; TAB remains invalid on human telnet input.

`SHOW PROP` HELP should stay aligned with the telnet formatter: the command
shows only rows whose `REL` prediction is `HIGH`, `MEDIUM`, or `LOW`.

## Notes For Documentation

- If HELP text changes here, the main README HELP block must change too.
- Numeric SSIDs on DX calls are stripped at spot construction boundaries; login
  calls remain distinct session identities while `SHOW OWN` and `WHOSPOTSME`
  use the baseline own call.
- Path-glyph notes should match the shipped glyph symbols from `data/config/path_reliability.yaml`.
- Dedupe HELP should match the effective secondary dedupe windows from `data/config`.

For session flow and filter behavior, see [`../telnet/README.md`](../telnet/README.md).
For preset ownership, contents, bounds and failure behavior, see
[`Named Presets`](../telnet/README.md#named-presets). The four commands
and their HELP topics are available in both dialects. Successful LOAD/SAVE
establish the applied/saved reference; failed association persistence after
SAVE retains the previous reference both live and on disk. Protected
temporary-defaults sessions cannot SAVE, LOAD or commit machine writes.
The 20-name, 256 KiB-per-preset and 8 MiB collection limits remain separate
from canonical, wire and decode-work bounds. Prospective admission checks the
complete retained profile and export sizes before accepting changes.

### WWV and WCY bulletins

`SHOW WWV` and `SHOW WCY` show the latest five retained bulletins of that type,
in receipt order. Add `FULL` to show its retained bulletins received in the
last 24 hours. Both dialects also accept `SH WWV`, `SHOW/WWV`, and `SH/WWV`,
with equivalent WCY forms. Add `FULL` after a space to any form. Numeric count
suffixes are not supported. Each type retains at most 48 bulletins, so an
active feed can fill the history before 24 hours have
elapsed. Warnings identify known or possible gaps. Archived bulletin text is
limited to 256 characters; longer text is marked as truncated.

Login shows the latest available WWV and WCY bulletin, and these commands show
requested bulletins, even if the corresponding live bulletin filter is off.
After login, live announcements follow the normal WWV and WCY filters. Receipt
time is when this cluster received the bulletin, not necessarily the time of the
underlying report. History is saved separately from the spot archive and can
start empty after installation or be incomplete after a storage failure or an
unclean shutdown.
For 24 hours after every startup, output warns that history may be incomplete:
a previous storage failure may have prevented recording a shutdown marker.

### Archive history

`SHOW DX` and `SHOW MYDX` search archived spots using your current filters and
existing self-spot rules. Both accept a count, a selector, or one of each in
either order. The default count is 50; the range is 1-250. `SH DX` and `SH MYDX`
are aliases in both dialects; `SHOW/DX` and `SH/DX` remain `cc`-dialect aliases.
Bare numbers are counts, not ADIF selectors.

```text
SHOW DX K1ABC 10
SHOW MYDX 10 K1ABC
SHOW MYDX 3D2/R 10
SHOW DX 20 COMMENT up 5
SHOW DX K1ABC 20 COMMENT POTA
SHOW DX K1ABC 20 BAND 20m,40m
SHOW DX K1ABC 20 MODE CW,FT8
SHOW DX K1ABC 20 MODE CW,FT8 BAND 20,40 COMMENT POTA
```

An exact canonical CTY label takes precedence and selects its whole ADIF entity;
`3D2/R` selects Rotuma. Otherwise a full valid call selects only its normalized
stored DX identity, and other supported prefixes select their CTY entity.
Queries and decoded old records use the same DX normalization, including numeric
SSID removal. An exact call is never widened to its country when no rows match.
A supplied selector requires loaded CTY, but a valid full call with unresolved
country remains a valid exact query. Conflicting canonical labels fail explicitly.
`SHOW DXCC` detail lookup retains its existing behavior.

Append `COMMENT <phrase>` after the optional selector/count to require a
case-insensitive literal substring of the stored spot comment. Selector/count
retain either existing order. The trimmed phrase must contain 1-64 printable
ASCII bytes; interior spaces, commas, quotes, `*`, `?`, `ALL` and `NONE` remain
literal text. Mode/report/time tokens removed during ingestion and diagnostics
added during display are not searched. This explicit selection is mandatory even
for self-spots; saved filters retain their existing self-spot exception. Searches
do not change saved rules. NEXT retains the original phrase and grammar.

Append `BAND <list>` and/or `MODE <list>` after the optional selector/count.
Lists require commas between values, allow spaces around commas, match any
supplied value within a category, and require all supplied categories. BAND and
MODE may appear in either order, once each; COMMENT must come last because
everything after it is literal phrase text.
Bands use existing normalization (`20` and `20m` are equivalent); modes use the
existing filter names/aliases (`PSK31` selects PSK), including `UNKNOWN` for blank
modes. Repeated values are deduplicated. `ALL`/`NONE` are unsupported in these
lists; omit a category to impose no additional query restriction for it.
Empty comma fields remain ignored. A supported configured mode alias named
`BAND`, `MODE` or `COMMENT` is a value at the start of the mode list or after a
comma. Otherwise a standalone keyword starts a clause. For example, with BAND/MODE
aliases mapped to CW, `MODE BAND` selects CW, `MODE CW,MODE,FT8` selects CW/FT8,
and `MODE CW BAND 20` selects CW on 20m. `MODE CW MODE FT8` is a repeated clause
and is rejected. Finish the list without a comma before starting another clause.
With COMMENT mapped to CW, `MODE COMMENT COMMENT POTA` selects CW and the
literal phrase POTA. Absent or hidden COMMENT aliases retain ordinary clause
behavior; comma-embedded COMMENT remains a list value and must be supported.
Missing commas, missing or unsupported values and repeated categories reject the
entire command without replacing the current search. Explicit BAND/MODE
restrictions apply even to self-spots, before counting, and narrow results within saved filters rather
than overriding them. The saved-filter self-spot exception remains unchanged.
NEXT retains all selections, and searches never change preferences. Only unique
canonical band/mode names are retained in the existing connection-owned cursor;
there is no new archive index or stored preference. See
[ADR-0262](../docs/decisions/ADR-0262-history-band-mode-selections.md) and its
[list grammar refinement](../docs/decisions/ADR-0263-history-list-comma-boundaries.md)
and [COMMENT alias boundary](../docs/decisions/ADR-0273-history-comment-alias-boundary.md).

Every history form applies the page's captured current time minus
`archive.retention_seconds`; the exact cutoff is included. Select newest matching
rows, then show that page chronologically. A page visits at most 200,000 archive
candidates, including malformed/decode-failed records, filter rejects and one
non-consuming older lookahead. At most 199,999 records are consumed. The work
limit reports incomplete search; reaching the requested count also offers a
continuation when older search remains.

Use the exact returned `SHOW DX NEXT H1...` command on the same connection.
`H1...` denotes the supplied H1-plus-32-hex-digit token, not a literal command
argument. Supported DX/MYDX aliases also accept NEXT. Each connection owns one
search; successful page publication rotates its token, so an old token cannot
be reused. Continued pages are marked as older history. Each page uses a fresh
archive view and current cutoff, not a snapshot frozen across commands.

Relevant filter/path-setting changes invalidate continuation, even if restored
to their previous values. Each page captures coherent settings; propagation
observations remain live. New valid searches replace the old search, even if
the new read fails; invalid fresh requests preserve it. A failed continuation
keeps its position retryable unless close, settings changes or a new search
independently invalidated it. Unreadable-record warnings persist across pages;
archive errors and unsafe malformed continuation boundaries are explicit errors.

For queue acceptance, disconnect and publication ownership, see
[telnet history](../telnet/README.md#archive-history-and-continuation). The reader
uses existing timestamp keys; no callsign index, backfill or migration is needed.
See [ADR-0251](../docs/decisions/ADR-0251-exact-call-paged-history.md) and
[TSR-0041](../docs/troubleshooting/TSR-0041-exact-call-history-and-scan-cap.md).

### Canonical DXCC input and display

History selection is described above; canonical labels retain precedence over
exact calls and prefixes.

Numbered DXDXCC/DEDXCC expressions accept positive ADIF integers and exact
canonical CTY labels in human and YAML requests:

```text
REJECT RULE 9 DXDXCC=IT9
REJECT RULE 8 DXCALL=IT9*
```

The first selects the whole ADIF entity; the second is a literal callsign-prefix
pattern. Labels resolve once per request and normalize to numeric codes, so the
first reads back as `DXDXCC=248`. Positive numeric operands need no CTY or membership
check; missing/nonpositive spot ADIF metadata is unknown. `SHOW DXCC` lookup
and archive selector precedence are separate, unchanged paths. See
[canonical DXCC prefixes](../telnet/README.md#canonical-dxcc-prefixes).

## US And Canadian State Commands And YAML Versions

DXSTATE and DESTATE are GO expression fields:

```text
PASS RULE 0 DXSTATE=CA,TX,ON,QC
REJECT RULE 9 DESTATE=AA,AE,AP
```

They use canonical observed registered-address metadata and the 60 FCC plus
13 Canadian codes, not operating location. Unknown/invalid State makes every
comparison false, including `<>`; NOT negates that result. See
[State provenance and archive metadata](../telnet/README.md#us-state-and-canadian-province-filters)
and `HELP RULE` for exact field rules.

In CC, DOS and DXSTATE select these same state/province codes through native
lists. Missing or invalid state fails PASS and survives REJECT. There is no
extra country gate. Native per-band geography uses schema-seven YAML.

In GO, NEARBY combines with ordinary NOT, AND and OR; it never removes geographic
conditions or changes dedupe. For example, `MODE=CW AND (DXCONT=EU OR NEARBY)`
selects CW from Europe or with a nearby endpoint. Missing proximity is false;
NOT NEARBY includes unknown proximity. The
[rule-only recovery contract](../telnet/README.md#grid-noise-and-nearby)
waives only mapping availability for an unchanged saved GRID with every other
setting/control unchanged. SELF/history exceptions and TOXIC remain distinct.

Machine schemas 6 and 7 have the separate compatibility boundaries described
above. Native disk version 7 retains both banks, independent controls and applied
baselines. The login-owned upgrade resets recognized older profiles' filter
banks to current operator starting defaults and all six switches in each bank
to DEFAULT. It preserves unrelated shared preferences and clears old applied
references. It never compiles old expressions merely to discard them.

Old named presets are cleared once per base-call library; new version-seven
presets survive retries and later numeric-SSID upgrades. Malformed or future
data is protected rather than overwritten. A failed library reset can leave
preset operations unavailable while an independent profile upgrade proceeds.
Durable reset notices retry at a later login opportunity after delivery failure;
the notice retry never repeats an already committed filter reset. See
[login upgrades](../telnet/README.md#login-and-session-behavior) and
[persistence and rollback](../telnet/README.md#persistence-format-and-rollback).

## MINSNR Commands

MINSNR category commands and maps are refused, not translated. In GO, use a
numbered SNR expression:

```text
REJECT RULE 1 SOURCE=SKIMMER AND MODE=CW AND SNR<8
```

This explicitly selects skimmer CW reports below 8, not an implicit human
exemption. SNR is known for any present report (`Spot.HasReport`), including
zero and human reports. Missing SNR comparisons remain false, including `<>`;
NOT reverses that result. Edit or clear the slot instead of a threshold map.
See [telnet minimum SNR](../telnet/README.md#minimum-snr).

## Comment Commands And Schema 4

```text
PASS RULE 0 BAND=20M,40M AND COMMENT="POTA"
REJECT RULE 9 COMMENT="CQ ""DX"""
SHOW FILTER RULE
```

Schema 4 and COMMENT-list mutations are refused. COMMENT now participates in
the same Boolean rules as every other expression field: each operand is a
1–64-byte printable ASCII literal substring with a non-space byte. Matching is
ASCII case-insensitive, not a regex/glob/word search. Preserve literal case,
punctuation and spaces; double quotes as `""` inside expression quotes, and
treat backslash literally. Go-quoted SHOW chunks are a different display
encoding, not expression syntax.

There are no saved phrase-list moves/removals or hidden-list preservation.
Replace the whole slot or use RESET FILTER RULE; rule changes participate in
preset/revision/history invalidation. See
[saved expressions versus explicit archive phrases](../telnet/README.md#comment-filters-and-searches)
and [schema 6](../telnet/README.md#client-yaml-configuration).
Explicit archive COMMENT/NEXT grammar above remains unchanged and mandatory
even for self-spots; it is not expression quoting.
