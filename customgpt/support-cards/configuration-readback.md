---
title: "Support Card: Configuration Readbacks And Client YAML"
description: Independent GO/CC banks, schema 7 and limited schema-6 access.
---

## Match

Use for filter/settings inspection, dialect switching, YAML configuration,
revision conflicts, upload errors, upgrade notices, protected records and preset
partial results. Retrieve with `getDoc` when source-map routing applies; this
card does not add an automatic Worker route.

## First Safe Check

Identify the active dialect, exact command/header and one redacted response.
Distinguish human edits, machine requests, preset operations and login notices.
Start with [Using CC](../../docs/cc-dialect.md),
[GO filters](../../telnet/README.md#filters) or the
[client protocol](../../telnet/README.md#client-yaml-configuration).
Local source behavior does not establish publication, deployment or validation.

## Must Include

- GO and CC remember independent filters and independent BEACON, SELF, TOXIC,
  ANNOUNCE, WWV and WCY selections. Switching activates the saved bank. GRID,
  noise, dedupe, PATHSAMPLES and SOLAR are shared. GO Boolean mutations require
  GO; CC uses native mode/source, geography and DXBM commands. Retrieve compiled
  HELP for exact aliases; do not translate arbitrary GO rules into CC.
- CC mode choices apply to HUMAN and SKIMMER. Source choices do not change modes
  or start feeds. Geography lists use OR within a category and AND across enabled
  categories; missing/invalid metadata fails PASS and survives REJECT. Global
  edits replace that category and clear its band exceptions, even if unchanged.
  Per-band geography uses GoCluster YAML; native CC User compatibility remains
  unverified. DXBM uses reported frequency and its fixed 15-band/37-slot table.
  Retrieve [CC commands](../../docs/cc-dialect.md#terminal-commands) for details.
- CC `SET/NOFILTER` restores current operator modes/sources and clears geography,
  band exceptions and DXBM while keeping six controls, GO and shared preferences.
  This differs from the one-time upgrade reset.
- GO `SHOW FILTER [RULE|FULL]` preserves exact expressions; CC `SHOW/FILTER`
  shows native selections/controls. `SHOW SETTINGS` separates configured,
  effective and session state. Human lines are at most 78 printable ASCII
  characters plus CRLF. Quoted display pieces are lossless display, not
  necessarily pasteable commands; concatenate decoded pieces without separators.
  Expression quotes instead double literal quotes.
- Human readbacks suppress spots through preparation, queueing and successful
  delivery, then for the reading interval. `RESUME` ends the pause; missed spots
  are not replayed. Machine GET and `SHOW HOLD` status/usage do not create pauses.
  Acknowledgements are not strict barriers for already admitted spots. See
  [read pause](../../telnet/README.md#read-pause).
- Live own-call spots require SELF and TOXIC, then bypass personal criteria.
  History's exception is independent of SELF, still obeying TOXIC and explicit
  query selectors. SELF does not mean reports the user submitted about others.

## YAML Ownership And Limits

Use `GET YAML CONFIG SCHEMA 7` for a complete writable both-bank configuration
backup. Restore only its `configuration` in a new envelope/current revision;
status, associations, baselines, notices and the library are not writable CONFIG.
FILTER owns the tagged active bank; SETTINGS owns shared preferences/selection.
Complete PUT/VALIDATE CONFIG describes both banks. Ordinary PATCH cannot supply
an inactive bank or combine filter editing with dialect selection. Retrieve
[YAML shapes and presence rules](../../docs/cc-dialect.md#yaml-and-backups).

Schema-seven writes use `PUT|PATCH YAML <resource> SCHEMA 7`; validation uses
`VALIDATE YAML CONFIG SCHEMA 7`. Bodies require matching `schema_version: 7`;
PUT/PATCH require the current `if_revision`.
PATCH replaces supplied mode/source/rule lists; named controls update separately.
Global geography clears that category's overrides; band-only PATCH edits named
rows, and an empty bands map changes none. DXBM PATCH supplies complete state.

Schema 6 is limited compatibility, not a complete backup. FILTER and CONFIG need
GO active; CONFIG proposals must remain in GO. SETTINGS/CAPABILITIES remain
available in CC with compatibility guidance, and SETTINGS can switch banks.
Do not present inactive GO as CC state. Legacy GET requires `SCHEMA 6`; legacy
upload headers retain no schema suffix and bodies require `schema_version: 6`.

| Boundary | Limit |
| --- | --- |
| Native CC canonical state | 128 KiB |
| Complete CONFIG or tagged preset | 192 KiB |
| Schema-seven upload/final frame and native CC human filter output | 256 KiB |
| GO human readback and schema-six upload/final frame | 65,536 bytes |
| Physical profile | 1 MiB |
| Shared preset library | 8 MiB and 20 names |

Complete encoding, metadata, both baselines and shared decode-work limits also
apply; maximal lists need not fit together. Input/output reservations can return
busy before preparation. Human lines retain their configured limit, 128 bytes
by default. Use YAML for long replacements; never split a replacing list.

Uploads use standalone `---`/`...` markers and a 30-second absolute reception
deadline. Unknown/read-only fields, nulls, contradictions, duplicate keys,
aliases, merges and extra documents are refused. Unsafe/incomplete/oversized
framing and malformed upload headers close the connection so tails cannot become
commands. Fully received invalid documents return framed errors. One preference
revision covers both banks, controls, shared settings and selection. Refetch
after conflict or reconnect; login/notice metadata does not advance it or history.

## Persistence, Presets And Upgrade Notices

Generation seven stores both banks in one profile. First recognized pre-seven
login discards old filters, resets all six controls in both banks to defaults
and clears associations/baselines. It preserves configured dialect, unrelated
settings and login metadata under their restore contracts. The shared library
clears old presets, including v6, once; new v7 presets survive later SSID upgrades
and retries. Each eligible reset replacement needs admission and a verified
original `.pre-v7.bak`; different existing backups are protected. Do not compile
old rules or edit version markers to migrate.

Profile reset, library reset and notice acknowledgement complete independently.
Library reset/marker share one commit. When both replacements are eligible,
the library commits first.
There is no two-file transaction: library failure can leave presets unavailable
while an independently safe profile reset succeeds; profile failure never undoes
a completed library reset. Historical notices report completed work. Full writer
flush gives a transmission opportunity, not proof of reading. Failed
acknowledgement can repeat the notice, never the reset. See
[ADR-0330](../../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md).

Machine writes and LOAD commit before live publication. Human late-save failures
retain both banks' active unsaved changes across switches, with warnings until a
complete save succeeds. Protected temporary sessions cannot overwrite refused
originals or SAVE/LOAD/apply machine writes. A timestamp/IP save warning after
successful restoration keeps restored preferences; it does not mean a reset.

SAVE stores the active bank/controls and shared preferences with a GO/CC tag.
LOAD restores that bank/shared preferences, announces a switch and keeps the
other bank. Each bank has an applied reference/baseline; shared edits can mark
either modified. Library overwrite/delete does not rewrite applied baselines.
SAVE may succeed in the library but fail its profile association; both previous
associations remain. Retrieve [presets](../../telnet/README.md#named-presets).

Stop writers before backup/repair/restore; one process owns a user directory.
Rollback needs matching binary, profiles and shared library. Preserve current
files first: restoring old backups loses later changes across affected SSIDs.
See [storage](../../data/config/README.md#user-configuration-storage).

## GO Boolean Rules

GO `PASS RULE`/`REJECT RULE` slots 0-9 replace whole expressions. REJECT vetoes;
PASS rules are ORed, and no PASS means unrestricted apart from REJECTs. Edit or
remove an existing PASS, including default slot 0, to narrow selection. GO
`RESET FILTER [RULE <slot|ALL>]` clears rules without reinstalling defaults or
changing controls/settings. A source-less PASS admits matching HUMAN/SKIMMER;
explicit SOURCE guards use ordinary Boolean logic. See
[GO filters](../../telnet/README.md#filters) and `HELP RULE`.

Unknown comparisons, including `<>`, are false; NOT negates them. Blank MODE is
known `UNKNOWN`, eventless spots use `EVENT=NONE`, the field is `CONF`, and PATH
classes match exactly. GO DXDXCC/DEDXCC requests accept positive ADIF integers
or exact canonical CTY labels; saved/readback rules are numeric. Keep callsign
patterns, entity comparisons, CTY history selectors and SHOW DXCC lookup distinct.
Retrieve [field semantics](../../docs/domain-contract.md#expressions-and-field-semantics)
and [DXCC distinctions](../../telnet/README.md#canonical-dxcc-prefixes).

## NEARBY

GO NEARBY is a zero-operand Boolean predicate; either endpoint can qualify in the
user's H3 cell. Missing required location is false, and NOT includes unknown
proximity. NEARBY does not suppress geography or choose dedupe. GRID/cells have
one client owner. Rule-only recovery waives only mapping availability for an
unchanged GRID with a supplied rules list and every other setting/control
unchanged. Syntax and other checks remain. Retrieve the
[NEARBY/recovery contract](../../docs/domain-contract.md#nearby-predicate-and-recovery)
for band resolution and effective path minimums. CC has no NEARBY category.

## US And Canadian State And Version Compatibility

GO DXSTATE/DESTATE and CC state categories use observed canonical metadata;
registered address does not prove operating location. Missing GO comparisons
are false; CC missing metadata fails PASS and survives REJECT. Archived empty
State is not hydrated from today's registry. Archive version 6 is separate from
configuration generation 7 and machine schemas. Retrieve
[State documentation](../../telnet/README.md#us-state-and-canadian-province-filters)
for FCC/ISED enrichment and refresh interruptions.

## MINSNR

This retained anchor routes former MINSNR questions to GO
[SNR expressions](../../telnet/README.md#minimum-snr), not saved threshold maps.
`REJECT RULE 9 SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10` explicitly limits
rejection to those skimmer reports. Without SOURCE, present human reports also
participate. Zero/negative reports are known; missing comparisons are false,
including `<>`, and NOT negates that. Native CC has no MINSNR category.

## Literal Comment Rules

GO atoms such as `COMMENT="CQ ""DX"""` use an ASCII case-insensitive literal
substring, not regex/glob/word matching. Operands are 1-64 printable ASCII bytes
with a non-space byte. Preserve punctuation/spaces; expression quotes double
quotes and backslash is literal. Edit/clear the whole slot, not a COMMENT list.
Retrieve [comment rules](../../telnet/README.md#comment-filters-and-searches)
and [rule limits](../../telnet/README.md#filters).

SHOW DX/MYDX explicit COMMENT is independent query grammar in either dialect and
mandatory even for self-spots. Follow returned NEXT commands. Mode aliases can
collide with BAND/MODE/COMMENT keywords; retrieve
[archive history](../../commands/README.md#archive-history) and
[ADR-0273](../../docs/decisions/ADR-0273-history-comment-alias-boundary.md).
A scan-budget stop is incomplete search, not proof of no match.

## Must Avoid

- Do not show GO rules as CC state, call schema 6 a complete backup or translate
  unsupported commands into another bank's edits.
- Do not promise native CC User/per-band terminal compatibility or invent OWN,
  BOB, DX/NODX, TALK, announcement/weather geography or independent WX controls.
- Do not upload status, invent force-revision/size-limit settings, reset protected
  files as first aid or equate library success with association success.
- Do not repeat resets to clear notices or equate successful flush with reading.

## Sources

- [CC banks, commands, YAML and recovery](../../docs/cc-dialect.md)
- [Current storage/bank decision](../../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md)
- [Human readbacks](../../telnet/README.md#human-configuration-readbacks)
- [Client YAML](../../telnet/README.md#client-yaml-configuration)
- [Named presets](../../telnet/README.md#named-presets)
- [Operator storage](../../data/config/README.md#user-configuration-storage)
- [Compiled HELP](../../commands/README.md#help-source-of-truth)
