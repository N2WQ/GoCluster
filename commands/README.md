# Commands And HELP

This directory is the source of truth for telnet command help. The public `HELP` output is built in [`processor.go`](processor.go), not copied from a separate static document.

The filter guidance here describes the Boolean MVP in the current local
checkout, not a production deployment or published-main source claim.
Private local-cluster acceptance remains pending.

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

The overview groups everyday commands by task and introduces numbered rules,
separate controls and a complete BAND/MODE recipe. `HELP RULE` is the canonical
Boolean expression, field, vocabulary, default/reset and bounds reference in
both GO and CC. `HELP PASS`, `HELP REJECT` and their RULE topics describe whole
slot replacement; PASS rules are ORed and matching REJECT vetoes them.
`HELP FILTERS` routes through `filterReferenceHelpLines`, which adds basics,
the recipe and separate controls rather than a second category manual.

`buildHelpCatalog(...)` in `processor.go` installs configuration HELP, COMMENT
HELP and then Boolean rule HELP before routing aliases. The registered
`BooleanRuleHelpLines` provider owns expression wording, not the unregistered
telnet `formatBooleanRuleHelp` helper. `configuration_help.go` owns human
readback and machine command topics; execution remains under telnet.

Separate controls retain their own grammar. Shared PASS/REJECT control forms
work in both dialects; CC SET/FILTER and UNSET/FILTER support BEACON, WWV, WCY,
ANNOUNCE/PC93, SELF, TOXIC and NEARBY only. Switches take no value or ALL.
NEARBY follows its explicit ON/OFF value with either verb. Only SET/FILTER
accepts `<switch>/ON|/OFF`, without extra values; UNSET and NEARBY have no
inline flags. Named CC control shortcuts retain their no-argument grammar.
Old category lists, MODE shortcuts, MINSNR, COMMENT-list, DXBM and NOFILTER
mutations are refused, not translated. Historical CC category mechanics in
[TSR-0064](../docs/troubleshooting/TSR-0064-cc-help-category-grammar.md)
are not a compatibility contract.

`HELP SYMBOLS` contains the existing confidence legend and the path legend when
its configured display is available. Configured glyphs and reporting windows
still come from startup snapshots; help does not read external text files.

YAML client commands are omitted only from the overview. They remain executable
and retain command-specific help, such as `HELP GET YAML CONFIG`.

HELP accepts a working command spelling with arguments, for example
`HELP SH DX 5`, `HELP SET SOLAR 30` or `HELP GET YAML CONFIG SCHEMA 5 ID read-1`.
It selects the longest complete command head without executing the command.
SH BUILD/OWN/DXCC/MYDX/PROP and history aliases resolve to their own topics;
shared PASS/REJECT topics also resolve in CC. Fixed CC shortcuts take priority
over loaded mode aliases. Unsupported heads remain explicit unknown topics;
HELP does not add executable aliases. See
[routing troubleshooting](../docs/troubleshooting/TSR-0065-help-routing-alias-coverage.md).

The main landing page [`../README.md`](../README.md) contains a generated HELP
block for the default `go` dialect, with a processor-output comparison test.
Regenerating that block is outside this documentation slice; it is not the
authority for this checkout's new Boolean contract.

Reviewed GO and CC overview fixtures live under [`testdata`](testdata).
Tests also check command discoverability, reference navigation and line widths.
The emitted `filterRecipeLines` recipe begins with `RESET FILTER RULE ALL`;
it deliberately removes every personal slot before installing its combined
BAND/MODE rule. `telnet/help_recipe_test.go` instead exercises a combined
slot-0 replacement retaining an independent REJECT. That different sequence
does not prove the emitted reset recipe preserves unrelated rules. These
tests were inspected, not run here. The pre-v5 category mechanics in
[ADR-0264](../docs/decisions/ADR-0264-task-oriented-compiled-help.md) are historical.

## Dialects

The runtime supports two command dialects:

- `go`: the default dialect shown in the main README
- `cc`: a DXSpider-style alias set with `SHOW/DX`, control-only `SET/FILTER`
  and `UNSET/FILTER`, and `SET/ANN`-style control shortcuts

Dialect selection is user-visible and persisted per callsign. HELP is rendered for the active dialect, so the command list and examples change with the selected dialect.

Bare PASS/REJECT and refused category mutations give bounded guidance toward
numbered rules and `HELP RULE`. The expression grammar is identical in GO/CC;
there is no category compatibility engine. PC93 remains an ANNOUNCE control
alias. Historical category discovery wording in
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
- `SHOW PROP`
- `SHOW BUILD`
  shows the startup-resolved UTC `YYMMDD` version, release tag, and Go toolchain
  when available; unstamped release tags are omitted;
  commit and build timestamp remain separate metadata in
  `--version` and PC18, rather than appearing in the version string.
- `SHOW OWN`
- `SHOW FILTER [RULE|FULL]`
- `HELP RULE`
- `PASS RULE <0..9> <expression>`
- `REJECT RULE <0..9> <expression>`
- `RESET FILTER RULE <0..9|ALL>`
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
- `RESET FILTER`
- `SAVE PRESET <name>`
- `LIST PRESET`
- `LOAD PRESET <name>`
- `DELETE PRESET <name>`
- `DIALECT`
- `BYE`
- `GET YAML FILTER|SETTINGS|CONFIG|CAPABILITIES SCHEMA 5 [ID <id>]`
- `PUT YAML FILTER|SETTINGS|CONFIG`
- `PATCH YAML FILTER|SETTINGS|CONFIG`
- `VALIDATE YAML CONFIG`

Filter mutation, named presets, configuration readbacks/uploads, `SHOW PROP`
execution, and read-pause state are handled in the telnet layer.
This package documents those commands in HELP, but the parsers for `PASS`,
`REJECT`, `SHOW FILTER`, `SHOW SETTINGS`, GET/PUT/PATCH/VALIDATE YAML, `SHOW PROP`,
`PAUSE`, `SHOW HOLD`, `RESUME`, and the `cc` aliases live under [`../telnet`](../telnet).

Setting a slot replaces its action and entire expression. Slots are not
priorities; another PASS broadens selection. No active PASS means unrestricted
personal selection except matching REJECTs. New accounts and old-profile resets
receive the same editable default PASS slot 0; restored Boolean empty rules stay empty.
A PASS rule without SOURCE allows HUMAN and SKIMMER.
SOURCE follows ordinary Boolean semantics, including OR and NOT.
Explicit SOURCE=HUMAN stays human-only, including saved default rules.
No saved expressions are rewritten. Shipped defaults now select both sources.
RESET FILTER RULE clears selected slots while preserving NEARBY; RESET FILTER
clears all slots and turns NEARBY OFF. Neither reinstalls defaults nor changes
separate controls/settings. See [the full rule contract](../telnet/README.md#filters).

`SHOW FILTER` and `SHOW FILTER RULE` show current settings, exact numbered
rules, effective expressions or suspension, and individual controls.
`SHOW FILTER FULL` adds geometry detail and preparation counts.
No view flattens Boolean alternatives into independent field lists.
`SHOW/FILTER` and `SH/FILTER` accept the same RULE/FULL arguments in both
dialects; category views are unsupported. `GET YAML FILTER SCHEMA 5` exposes
machine-readable expressions and exact DEFAULT/boolean controls.
`SHOW SETTINGS`
separates configured preferences and effective behavior from session status.
Path minimums reflect active runtime state for both stations and beacons.
Both views report the associated preset and whether preferences differ from
its retained reference snapshot.

Human lines contain at most 78 printable ASCII characters followed by CRLF.
Exact strings use Go-style quoted ASCII escapes; complete quoted pieces joined
by `+` preserve long values without trimming spaces or splitting escapes.
This is display encoding, not expression quoting or necessarily a copyable
command. Slots display in numeric order, not priority order. Human input
retains the configured limit (128 bytes by default); use schema 5 YAML for
longer expressions. Rendering reads retained immutable prepared rules, not
render-time compilation or an additional category gate.

These human readbacks always suppress live spots during preparation, queueing
and delivery, followed by a full reading interval after the server write and
flush. They use the positive configured duration or 30 seconds when it is
zero, ignoring the row threshold even when disabled. Longer existing pauses
are preserved; later valid PAUSE/RESUME controls supersede earlier pending
completion effects. Human size errors follow the same pause policy.

Client YAML commands work in both dialects and never change pause state,
including on success or error. Each reply is one complete framed control
message with no human footer. GET exposes exact writable configuration
separately from read-only status. Every GET requires SCHEMA 5, including
SETTINGS and CAPABILITIES. CAPABILITIES advertises
`supported_schema_versions: [5]`, rule fields/operators/vocabularies and bounds,
not the old settings-availability matrix; runtime settings admission is
authoritative. Rule status includes configured/effective/suspended entries and
full/nearby/retained counts. Optional GET IDs preserve case and accept 1-32 ASCII
letters, digits or hyphens.

FILTER has exactly eight fields: `rules`, `nearby_enabled`, `include_beacons`,
`allow_wwv`, `allow_wcy`, `allow_announce`, `allow_self`, `allow_toxic`.
SETTINGS retains its six fields: `dialect`, `grid`, `noise_class`,
`dedupe_policy`, `path_min_observation_count`, `solar_summary_minutes`.
CONFIG nests `filters` and `settings`. Each rule requires integer slot 0–9,
lowercase action pass/reject and a complete `expr`.

PUT requires the complete resource. VALIDATE accepts CONFIG only and requires
complete filters/settings without applying or saving. PATCH retains omitted
fields; a supplied rules list replaces all slots, not just those supplied,
and `[]` clears it. Preserve DEFAULT, explicit false/true, empty default
strings and zero sentinels, subject to admission.

Upload headers have no schema suffix. The strict envelope permits
`schema_version`, `request_id`, `if_revision` and `configuration`; bodies require
`schema_version: 5`, request_id and configuration. PUT/PATCH also
require the revision from GET as if_revision; VALIDATE permits it optionally.
GET again after reconnect or a conflict. Validation or persistence failure
leaves live and saved configuration
unchanged; unavailable choices are rejected. Even an unchanged PUT persists
before success without resetting unrelated runtime state. Writes preserve the
preset reference and can set or clear modified. Omitted/old schemas, category
fields, unknown/read-only fields, duplicate keys/slots, aliases, anchors,
merges, custom tags, nulls and extra documents are refused without translation.

Bounds are 10 slots, 1,024 raw and canonical expression bytes, 32 atoms/128
nodes/depth 8 per rule, 32 values per atom and 128 atoms/512 values total.
COMMENT operands are 1–64 printable ASCII bytes with a non-space byte.
Parentheses/NOT count toward nesting, not flat AND/OR height. Machine,
human-line, storage and decode-work bounds are separate.

Every new readback has a 65,536-byte final response limit, including CRLF,
framing and human footers, with complete output or an explicit error. Upload
bodies use a separate 65,536-byte limit, excluding markers but counting actual
LF/CRLF bytes, and a 30-second deadline from valid-header acceptance.
Malformed upload headers, oversize, expiry and unreliable/incomplete framing
close the connection without dispatching payload remnants. A fully received
invalid document gets a YAML error and keeps the connection open. Ordinary
command headers keep their configured limit (128 bytes in the shipped config).

See the [human display contract](../telnet/README.md#human-configuration-readbacks)
and [complete schema-5 examples and framing](../telnet/README.md#client-yaml-configuration).

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
from the readback budget, so a valid large preset may LOAD even when FULL/YAML
readback returns a size error.

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

Numbered DXDXCC/DEDXCC expressions accept positive ADIF integers only:

```text
REJECT RULE 9 DXDXCC=248
REJECT RULE 8 DXCALL=IT9*
```

The first selects the whole ADIF entity; the second is a literal callsign-prefix
pattern. Neither performs CTY lookup. Positive operands need no CTY membership
check; missing/nonpositive spot ADIF metadata is unknown. `SHOW DXCC` lookup
and archive selector precedence are separate, unchanged paths. See
[canonical DXCC prefixes](../telnet/README.md#canonical-dxcc-prefixes).

## US And Canadian State Commands And YAML Versions

DXSTATE and DESTATE are expression fields in both dialects:

```text
PASS RULE 0 DXSTATE=CA,TX,ON,QC
REJECT RULE 9 DESTATE=AA,AE,AP
```

They use canonical observed registered-address metadata and the 60 FCC plus
13 Canadian codes, not operating location. Unknown/invalid State makes every
comparison false, including `<>`; NOT negates that result. See
[State provenance and archive metadata](../telnet/README.md#us-state-and-canadian-province-filters)
and `HELP RULE` for exact field rules.

NEARBY structurally removes only geographic leaves, collapses surviving
branches and ignores entirely emptied rules. Full saved rules stay editable;
OFF uses the latest full expressions, not an old snapshot. The
[rule-only recovery contract](../telnet/README.md#grid-noise-and-nearby)
cover already-ON restored unusable geometry. Enabling/grid changes still
require usable geometry; SELF/history exceptions and TOXIC remain distinct.

Only explicit machine schema 5 and native disk version 5 are supported here.
Records, library entries and applied baselines use versioned nested
configuration. Old machine schemas remain refused. A separate login-owned
reset-only upgrade for recognized old profiles preserves settings and installs
the same configured mode/source defaults as new accounts; it does not translate
old filter rules. See [login upgrades](../telnet/README.md#login-and-session-behavior).
Old/unversioned/future records are refused/protected. No reset or preset wipe
was executed here. See [native v5 storage](../telnet/README.md#persistence-format-and-rollback).

## MINSNR Commands

MINSNR category commands and maps are refused, not translated. Use a numbered
SNR expression:

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
and [schema 5](../telnet/README.md#client-yaml-configuration).
Explicit archive COMMENT/NEXT grammar above remains unchanged and mandatory
even for self-spots; it is not expression quoting.
