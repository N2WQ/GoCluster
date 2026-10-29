---
title: "Support Card: Configuration Readbacks And Client YAML"
description: Support boundaries for human configuration readbacks and schema-6 requests.
---

## Match

Use for human SHOW FILTER/RULE/FULL or SHOW SETTINGS output, canonical
GET/PUT/PATCH/VALIDATE YAML configuration commands, revision conflicts, upload
errors, protected temporary records, preset modification status or partial SAVE.
If `getSupportRoute` is not decisive, follow source-map or troubleshooting links
and retrieve this card with `getDoc`; it is not a new automatic Worker route.

## First Safe Check

Identify whether this is a human readback, a machine upload/readback, a preset
operation or a login warning. Retrieve the relevant section of `telnet/README.md`
before interpreting the exact command and response. Ask for one redacted
command/error artifact that distinguishes the next step.

## Must Include

- Login and SHOW FILTER use actual settings and numbered expressions, never
  independent lists that lose AND/OR/NOT relationships. New accounts see
  "Default settings and filters"; returning accounts see "Current settings and
  filters". Legacy reset users retain settings and get a short reset notice.
  Old-library cleanup alone is not a profile reset. Source eligibility is not
  a feed-connection status. Short rules use command syntax; long exact display
  pieces require decoding or YAML, not direct pasting.

- Personal selection uses one prepared Boolean engine, not a category
  compatibility gate. Both dialects use `PASS RULE <0..9> <expression>`,
  `REJECT RULE <0..9> <expression>` and `HELP RULE`. Setting a slot replaces
  its action and whole expression. Matching REJECT vetoes; PASS rules are ORed.
  No active PASS means unrestricted personal selection except matching REJECTs.
  Slots are not priorities. Adding PASS broadens selection; edit or remove an
  existing PASS, including default slot 0, to narrow it. See
  [Filters](../../telnet/README.md#filters).

- New accounts and old-profile resets receive the same configured MODE/SOURCE
  selection as editable PASS slot 0. Existing/restored Boolean empty rules stay empty.
  A PASS without SOURCE allows HUMAN and SKIMMER. SOURCE comparisons use normal
  Boolean logic. Explicit SOURCE=HUMAN stays human-only, including saved defaults.
  The shipped starting selection now includes both sources. Existing profiles
  and presets are not rewritten; source-less saved PASS rules now admit matching
  skimmer reports too. Neither PASS nor REJECT adds an implicit source condition.
  `RESET FILTER RULE <0..9|ALL>` clears selected slots;
  `RESET FILTER` clears all rules, including their NEARBY terms. Neither reinstalls
  defaults nor resets separate controls/settings. Old category, MODE-delta,
  MINSNR, COMMENT-list, DXBM and NOFILTER mutations are refused, not translated.
  There are no AR-style rule aliases or translators.

- HELP accepts source-verified aliases and command arguments without executing
  them: SH BUILD/OWN/DXCC/MYDX/PROP, shared CC PASS/REJECT and both-dialect slash
  filter readbacks. The longest complete head wins; unsupported names remain
  unknown. Fixed CC shortcuts take precedence over loaded mode aliases. See
  [TSR-0065](../../docs/troubleshooting/TSR-0065-help-routing-alias-coverage.md).

- BEACON, WWV, WCY, ANNOUNCE/PC93, SELF and TOXIC remain separate controls,
  not expression fields. Shared PASS/REJECT switches take no value or ALL;
  NEARBY is a bare expression predicate; retired toggle forms are refused. CC SET/FILTER and
  UNSET/FILTER retain controls only. Retrieve current
  [commands HELP ownership](../../commands/README.md#help-source-of-truth)
  for exact control/alias grammar. The category mechanics in
  [TSR-0064](../../docs/troubleshooting/TSR-0064-cc-help-category-grammar.md)
  and [TSR-0066](../../docs/troubleshooting/TSR-0066-command-discovery-wording.md)
  are historical, not current mutation instructions. Spot queued confirms
  intake, not delivery.

- Stored user records have separate 1 MiB raw, normalized and exact-write limits;
  configuration and baseline each use the 256 KiB standalone-preset encoding.
  Expensive alias expansion can also be refused; decoding-work units are not
  file or heap bytes. Preserve refused files for backup/repair. Initial protected
  defaults remain protected for the session; a warm refusal allows admitted
  temporary changes with a warning and retries a repaired authoritative record
  on later saves or ordinary logout. A prospective oversized replacement rejects
  before publication. SAVE admits library and association before either write;
  only a later association I/O failure can produce partial success. Retrieve
  [native storage](../../data/config/README.md#user-configuration-storage);
  [ADR-0270](../../docs/decisions/ADR-0270-complete-storage-admission.md)
  records admission history, not a pre-v5 migration contract.

- DXDXCC/DEDXCC human and YAML request operands accept positive ADIF integers
  and exact canonical CTY labels, not callsign lookups. `DXDXCC<>K` normalizes
  to `DXDXCC<>291`; `DXDXCC=IT9` selects entity 248 while `DXCALL=IT9*` is a
  literal callsign-prefix pattern. Labels need CTY; unknown/conflicting labels
  refuse the whole edit, including rules with NEARBY. One snapshot covers the request.
  Readbacks and saved rules are numeric; native loaders remain numeric-only.
  Numeric-only requests and saved-rule reload need no CTY. SHOW DX/MYDX retain CTY-label precedence and numeric
  count arguments; SHOW DXCC lookup is unchanged. Route to
  [DXCC distinctions](../../telnet/README.md#canonical-dxcc-prefixes).

- History MODE keyword aliases use the loaded filter-visible vocabulary.
  BAND/MODE/COMMENT aliases are values first or after a comma; otherwise a
  standalone keyword starts a clause. With COMMENT mapped to CW,
  `SHOW DX MODE COMMENT COMMENT POTA` selects CW and literal POTA text.
  Absent/hidden aliases retain ordinary COMMENT clause behavior. A refused
  request preserves the previous search and preferences; copy returned NEXT
  commands. Retrieve [ADR-0273](../../docs/decisions/ADR-0273-history-comment-alias-boundary.md)
  or [TSR-0063](../../docs/troubleshooting/TSR-0063-history-comment-alias-boundary.md)
  for boundary and checker details; do not infer production archive/model quality.

- `SHOW FILTER` and `SHOW FILTER RULE` show current settings, exact
  canonical numbered rules and individual controls. `SHOW FILTER FULL`
  adds geometry detail and preparation counts. SHOW/FILTER and SH/FILTER retain RULE/FULL readback aliases;
  category views are unsupported. SHOW SETTINGS separates configured selections,
  effective choices and session status; both views show the preset.
  [Human readbacks](../../telnet/README.md#human-configuration-readbacks)
  own the output contract. Rendering reads immutable prepared rules; it does
  not compile expressions or apply another category gate.
- Missing/unknown values make every comparison false, including `<>`; NOT
  negates that result. Blank MODE is known `UNKNOWN`; eventless spots use
  `EVENT=NONE`. The expression field is `CONF`, not `CONFIDENCE`, and is unknown
  for confidence-exempt modes. PATH classes match exactly: UNLIKELY excludes
  CLOSED; `PATH=UNLIKELY,CLOSED` selects both. SNR is known for any present
  report, including human and zero reports. Retrieve `HELP RULE` and
  [field semantics](../../docs/domain-contract.md#expressions-and-field-semantics)
  rather than applying old category exemptions.
- Every human line is at most 78 printable ASCII characters followed by CRLF.
  Exact strings use quoted ASCII escapes. Complete quoted pieces joined by +
  preserve long values without trimming whitespace or splitting escapes.
  These are Go-style display quotes/escapes, not expression quoting or
  necessarily copyable commands. Decode each piece and concatenate without
  separators; indentation, line endings and + are not stored characters.
  Expressions instead double literal quotes and keep backslashes literal.
  Every complete response must fit 65,536 bytes or return an explicit size
  error. Human input keeps the configured limit, 128 bytes by default;
  use schema 6 YAML for longer expressions.
- Effective path minimums come from restored runtime state and loaded server
  configuration. With station/beacon floors 21/11, reconnect leaves a saved
  personal minimum of 15 inactive, so effective values remain 21/11. An active
  personal minimum of 30 gives 30/30. Explain disabled/unavailable prediction.
- NEARBY is a zero-operand predicate with ordinary NOT/AND/OR logic. It does not
  suppress geography, change SOURCE or choose dedupe. Either endpoint may share
  the user's H3 cell; 160m/80m/60m use coarse resolution and other supported bands
  fine resolution. The helper is preserved exactly: empty/??? after fallback
  makes the band unavailable; unsupported nonempty labels retain fine behavior.
  Missing required location or no matching endpoint makes NEARBY false.
  NOT NEARBY includes unknown proximity, not proof of distance. Other OR branches
  remain usable. See [NEARBY](../../telnet/README.md#grid-noise-and-nearby).
- GRID/provenance/cells have one client-configuration owner. Unrelated edits
  preserve location without repeating callsign lookup. PATH/display enablement
  does not control NEARBY. No NEARBY/PATH demand means no new capture mapping.
  A supplied rules list can be edited through PUT/PATCH/VALIDATE with all other
  settings/controls unchanged even if GRID is unmappable. Only mapping is waived;
  syntax and every other setting check remain. Human rule edits need no location.
  Preserve SELF/history exceptions, TOXIC and mandatory explicit history clauses.
  Route to [recovery](../../docs/domain-contract.md#nearby-predicate-and-recovery)
  rather than weakening rules. TSR-0109's old independent cell-pair contract is
  retired by [ADR-0329](../../docs/decisions/ADR-0329-nearby-boolean-predicate-and-generation-six.md).
- Human readbacks, including size errors, suppress spots before preparation,
  through queueing/delivery and for the full interval after successful write
  and flush. They ignore zero row thresholds and use 30 seconds for a zero
  duration. Longer pauses remain; later processed PAUSE/RESUME takes precedence.
  Pending delivery is not an already-running reading countdown; the SETTINGS
  snapshot describes the full interval that follows server delivery.
- SHOW HOLD accepts exactly two tokens with case-insensitive ASCII-space
  separation; extra tokens return handled usage without pause-state effects.
  Its status and usage replies bypass automatic pause. SH HOLD and slash forms
  remain unsupported, and TAB remains invalid human wire input. Suppression
  counts describe eligible live traffic, not a command effect or cluster total.
- Every machine GET requires explicit `SCHEMA 6`, including SETTINGS and
  CAPABILITIES: `GET YAML FILTER|SETTINGS|CONFIG|CAPABILITIES SCHEMA 6 [ID <id>]`.
  Machine success/errors have no pause-state effects or human footer. Existing
  suppressed counts may still increase from live traffic during a prior pause.
- GET separates exact writable configuration from read-only status. Preserve
  explicit false, zero, empty values/collections and DEFAULT selections. GET IDs
  are 1-32 ASCII letters/digits/hyphens and preserve case; CAPABILITIES is read-only.
- PUT/PATCH headers have no schema suffix; bodies require `schema_version: 6`.
  PUT requires a complete FILTER, SETTINGS or CONFIG resource. PATCH retains
  omissions; a supplied `rules` list replaces all slots, and `[]` clears it.
  It is not a per-slot merge. VALIDATE YAML CONFIG checks a complete proposal
  without applying or saving it. Rule entries require slot, lowercase action
  pass/reject and expr. Missing/old schemas and category fields are refused,
  not translated or preserved as hidden maps. Retrieve
  [the client protocol](../../telnet/README.md#client-yaml-configuration)
  for the strict writable envelope, field sets and examples. Duplicate
  keys/slots, unknown/read-only fields, nulls, anchors, aliases, merges,
  custom tags and extra documents fail unchanged.
- `GET YAML CAPABILITIES SCHEMA 6` advertises `supported_schema_versions: [6]`,
  rule vocabulary/operators and bounds, not the old settings-availability
  matrix. Runtime settings validation and effective node config remain
  authoritative. NEARBY is advertised in rule_predicates with operand_count 0
  and operators []; comparison fields remain separate. Rule status contains
  configured_count, one program count mapping, and canonical slot/action/expr
  entries. Retired nearby_enabled/nearby_active/projected/suspended fields are absent.
  Even a false nearby_enabled proposal is refused.
- PUT/PATCH use GET's matching revision as if_revision. GET again after reconnect,
  restart or conflict. Validation/persistence failure leaves live and saved
  configuration unchanged; unavailable choices are rejected. Even an unchanged
  PUT persists before success, repairing failed human autosave without resetting
  solar scheduling, GRID-owned state, diagnostics or pause.
- Every new readback's 65,536-byte final budget includes CRLF, markers and human
  footers. Complete output or an explicit error replaces silent truncation.
  Machine proposals must fit a complete CONFIG readback with reserved metadata;
  valid larger presets may still LOAD under the separate 256 KiB preset limit.
- Uploads use standalone ---/... lines, 65,536 actual body bytes excluding
  markers, and a 30-second absolute deadline from valid-header acceptance.
  Malformed upload headers and oversized/expired/incomplete/unreliable framing
  close the connection without dispatching remaining bytes. Fully received
  invalid documents return framed YAML errors and leave the connection open.
  Terminal rejection also skips final preference autosave, preserving the disk
  record even when an earlier human command changed live state but failed to save.
- Unreadable/unsupported user records are preserved. Protected temporary-defaults
  sessions allow readbacks and temporary human changes but reject SAVE before
  any library write, LOAD and PUT/PATCH. A login timestamp/IP save warning after
  successful restoration instead continues with the restored preferences.
- Preset modified compares current preferences with the retained applied/saved
  reference for the full callsign/SSID. Reversing changes clears it; PAUSE,
  and diagnostics do not affect it. Library overwrite or
  deletion does not change the reference. Partial SAVE retains the previous
  association/reference live and on disk, including subsequent ordinary saves.

## Must Avoid

- Do not offer callsign lookup keys as canonical DXCC labels, prefix-bearing
  native saved files, category mutation compatibility, AR-style aliases or
  automatic conversion. Keep literal callsign patterns,
  ADIF entity comparisons, CTY history selectors and SHOW DXCC lookup separate.

- Do not invent SHOW YAML aliases, pagination, a force-revision command or an
  operator setting for raising these limits.
- Do not upload read-only status or omit required PUT fields to mean defaults.
- Do not silently substitute an unavailable choice, promise memory-only success
  or skip persistence because a PUT matches the current live configuration.
- Do not claim a malformed upload tail can become ordinary commands, or confuse
  the ordinary 128-byte shipped command-header limit with the upload-body limit.
- Do not reset/delete a protected record or overwrite a named preset with
  temporary defaults as a first troubleshooting step.
- Do not treat a saved library snapshot as proof that its new SSID association
  persisted, or treat all SSIDs as one session configuration.
- Do not apply category map/exemption semantics to expressions or calculate
  effective path minimums from a saved preference reconnect did not activate.
- Do not strip quoted spaces, join wrapped pieces with a separator, or treat
  the human effective format as the client YAML schema.

## US And Canadian State And Version Compatibility

DXSTATE/DESTATE compare observed canonical metadata against 60 FCC mailing codes
and 13 Canadian province/territory codes. Missing/invalid State is unknown:
`DXSTATE<>CA` is false, while `NOT DXSTATE=CA` is true. A reject-only set may
admit unknown State because no rejection matched. NEARBY combines with State
through ordinary Boolean logic; it does not suppress geography. Registered address does not establish
operating location; do not infer it from a portable prefix, grid or call district.
Enrichment remains active when `fcc_uls.enabled` is false: that flag disables
license rejection only. Missing/unavailable state stays empty.
FCC extraction/rebuilding and ISED publication make their own cached calls
unavailable. Apply unknown/NOT expression semantics, not an implicit PASS/REJECT
exemption; archived empty State stays empty after refresh. See the
[State filter documentation](../../telnet/README.md#us-state-and-canadian-province-filters)
for the refresh interruption and its measured local duration.

Machine schema 6 is explicit on every GET. Native records require explicit
`configuration_version: 6` and nested `configuration.filters/settings`.
Named library entries and applied baselines each require version 6; a library
has a `presets` mapping, not a top-level version or machine envelope.
Pure v6 decoding refuses older formats. Login separately upgrades recognized
unversioned/version 1-5 profiles once, preserving settings, separate controls
and metadata while installing the same configured mode/source rule as a new account.
Old preset entries/associations are cleared; existing v6 presets survive.
Original .pre-v6.bak backups precede replacement; prior .pre-v5.bak files are
untouched. Only initial authoritative profile absence permits new-user creation.
Library replacement precedes profile replacement with truthful partial outcomes
and safe retry, preserving newly created v6 presets. Explicit disk zero, future, malformed
and unsafe inputs remain protected. No version-marker edit or old-rule
translation is supported. See [ADR-0318](../../docs/decisions/ADR-0318-old-profile-reset-upgrade.md).
Stop writers before backup/repair/restore; one writer process owns the data
directory. Atomic replacement writes, syncs and closes its temporary file first.
Downgrades require matching binary/data backups.

Archive version 6 separately stores observed State; older archive versions 2–5
have unknown State, without current-registry hydration. This archive metadata
history is not configuration migration. Failed FCC imports retain the last good
FCC database. Route storage details to
[User Configuration Storage](../../data/config/README.md#user-configuration-storage).

## Sources

- [telnet/README.md human readbacks](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#human-configuration-readbacks)
- [telnet/README.md client schema and framing](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#client-yaml-configuration)
- [telnet/README.md named presets and persistence](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#named-presets)
- [commands/README.md HELP ownership](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md)
- [data/config/README.md effective node YAML](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md)
- [Boolean personal-rule contract](../../docs/domain-contract.md#boolean-personal-rules)

The Boolean/v6 routes describe the current local MVP, not production deployment
or completed private local-cluster acceptance. That acceptance remains pending. New source
owners named here are local checkout paths, not claims of publication on main.

Canadian licensing uses the required `ised` configuration block and independent
paired ISED snapshot. `ised.enabled: false` keeps downloads and enrichment on.
The same DXSTATE/DESTATE fields accept all 13 province/territory codes alongside
the 60 FCC codes. Base-call CTY selects source, including foreign portable calls.
Events use inclusive UTC dates; a prefix match means callsign plausibility,
without proving eligibility. History uses stored State. Archive version 6 is
separate from native configuration v6 and machine schema 6. For source/failure
details, see [ADR-0254](../../docs/decisions/ADR-0254-canadian-ised-license-and-state-reuse.md)
and [configuration](../../data/config/README.md#fcc-and-ised-reference-data-and-enforcement);
any pre-v5 category/schema mechanics in the ADR are historical.

## MINSNR

This retained heading routes former MINSNR questions to
[Boolean SNR expressions](../../telnet/README.md#minimum-snr), not a saved
threshold map. For example, `REJECT RULE 9 SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10`
explicitly limits rejection to skimmer reports below 10. Without that source
guard, present human reports also participate. Zero/negative reports are known;
missing reports make comparisons false, including `<>`, and NOT negates that.
[ADR-0258](../../docs/decisions/ADR-0258-per-mode-minimum-snr-filter.md)
records historical pre-v5 category mechanics, not current exemptions or schema
compatibility. Rule changes retain preset/revision/history invalidation and
the human-versus-machine publication boundaries above.

## Literal Comment Rules

Retrieve [the comment contract](../../telnet/README.md#comment-filters-and-searches)
for saved atoms such as `COMMENT="CQ ""DX"""`. Each operand is 1–64 printable
ASCII bytes with a non-space byte; matching is an ASCII case-insensitive literal
substring, not regex/glob/word matching. Preserve case, punctuation and spaces;
expression quotes double literal quotes, and backslash is literal.
Use SHOW FILTER RULE or `GET YAML FILTER SCHEMA 6`. Edit/clear the whole slot,
not a COMMENT list. Saved/effective readbacks consume retained immutable
programs; no phrase-list compatibility is supported.

Rule budgets are ten slots, 1,024 raw and canonical bytes per expression,
32 atoms/128 nodes/depth 8 per rule, 32 values per comparison atom and 128 atoms/512 values
total. Depth counts parentheses/NOT, not flat AND/OR height. These do not
replace human-line, machine, stored-byte or decode-work bounds. NEARBY consumes
one atom/node and zero values; retrieve
[the primary limits](../../telnet/README.md#filters) before diagnosing refusal.

SHOW DX/MYDX accepts COMMENT after selector/count options. Its phrase is mandatory
even for self-spots, while saved filters retain existing self exceptions. Follow
the returned NEXT command; comment edits invalidate continuation. A rare match
may exhaust a page's scan budget, which is incomplete search rather than absence.
See [archive history](../../commands/README.md#archive-history).
[ADR-0261](../../docs/decisions/ADR-0261-literal-comment-filter-and-history.md)
records historical saved-list mechanics; explicit archive phrase grammar is
separate and unchanged.
