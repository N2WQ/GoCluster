# Using the CC dialect

GO and CC remember separate filters and separate BEACON, SELF, TOXIC, ANNOUNCE,
WWV and WCY choices. `DIALECT CC` restores your CC selection; `DIALECT GO`
restores your GO rules. GRID, noise, dedupe, PATHSAMPLES and SOLAR remain shared.
Switching can change which spots you receive.

Both banks start with the operator's current mode/source defaults. The upgrade
clears all earlier filters, six switch selections and old named presets. It
preserves selected dialect and unrelated settings. New presets saved after the
shared library reset survive later SSID upgrades and retries.

## Terminal commands

| Command | Effect |
| --- | --- |
| `SET/CW`, `SET/FT8`, other enabled mode shortcuts | Include that mode from both human and skimmer reports. |
| `SET/NOCW`, `SET/NOFT8`, corresponding `UNSET/<mode>` | Exclude that mode from both sources. |
| `SET/SKIMMER`, `SET/NOSKIMMER` | Include/exclude skimmer reports without changing modes or starting a feed. |
| `SET/FILTER DOC/PASS K,VE` | Replace spotter-country selection with US or Canada. |
| `SET/FILTER DOC/REJECT K` | Replace that category with rejection of US spotters. |
| `SET/FILTER DOC/OFF` | Remove only the spotter-country restriction. |
| `SET/FILTER DOS/PASS NY,ON` | Replace spotter-state/province selection. |
| `SET/FILTER DXCTY/PASS JA` | Replace spotted-station country selection. |
| `SET/FILTER DXSTATE/REJECT CA` | Replace spotted-station state/province rejection. |
| `SET/FILTER DXBM/PASS 6` | Include the named native band; first PASS after OFF selects only named slots. |
| `SET/FILTER DXBM/REJECT 6` | Exclude named slots; first REJECT after OFF allows everything else. |
| `SET/FILTER DXBM/OFF` | Remove only DXBM restrictions. |
| `SET/NOFILTER` | Restore current operator modes/sources and clear geography, band overrides and DXBM. Keep the six switches. |
| `SHOW/FILTER`, `SH/FILTER [category]` | Inspect native selections and six switches. |
| `SHOW/SETTINGS`, `SH/SETTINGS` | Inspect shared preferences and current session status. |
| `HELP SET/FILTER` | Read the compiled native command reference. |

Country lists accept canonical numeric ADIF identifiers or resolvable country
prefixes. Lists replace the named category, rather than accumulating. Thus
`DOC/PASS K` followed by `DOC/PASS VE` leaves Canada only. Categories combine:
`DOC/PASS K` plus `DXCTY/PASS JA` selects US spotters reporting Japan. A missing
or invalid value fails PASS and survives REJECT. Valid state tokens use the
existing state reader; no extra nationality test is applied.

An all-band category edit clears that category's earlier band exceptions, even
if the global value is unchanged. Use schema-seven GoCluster YAML for per-band
geography. A missing band row inherits global; an explicit OFF row removes only
that band's restriction. Native CC User per-band command syntax is unverified.

DXBM uses the fixed CC 15-band/37-slot frequency table, including MW. The reported
frequency determines the slot. At a shared endpoint the upper slot owns the
frequency; the band's final upper endpoint is included. Later edits change only
named slots; a whole-band edit changes all its slots. OFF and an active selection
that currently allows everything remain distinct for the next edit. These
ordering rules are explicit GoCluster choices, not verified native CC behavior.

Boolean rule edits require GO. OWN, BOB, DX/NODX, TALK filtering, announcement
and weather geography, and an independent WX switch are unsupported. ANNOUNCE
retains its existing inclusion of WX. Unsupported operations change nothing.
The operator taxonomy determines available mode shortcuts; YAML exposes the
explicit mode list, including modes without a terminal shortcut. Fixed CC words
and the negative `NO` prefix keep their grammar when a custom taxonomy token
collides with them; use the YAML mode list for that token.

Human filter/settings output pauses spots while it is delivered and for the
reading interval afterward. `RESUME` ends the pause. Missed spots are not replayed.
Machine GET does not pause spots. Already queued spots retain existing delivery
semantics; a filter update is not a strict wire barrier.

SELF retains the existing own-call exception: live spots of your normalized
login call require SELF and TOXIC, then bypass personal criteria. History's
own-call exception is independent of SELF, still obeys TOXIC and explicit search
selectors. SELF does not mean spots you reported about another station.

## YAML and backups

Use `GET YAML CONFIG SCHEMA 7` for a complete writable configuration backup.
It includes `configuration.filters.go`, `configuration.filters.cc` and shared
`configuration.settings`. Read-only status, associations and reset metadata are
not writable backup fields. Restore the writable `configuration` in a new
request envelope with the current revision; do not upload the GET status.

| Surface | Ownership |
| --- | --- |
| `GET YAML FILTER SCHEMA 7` | Tagged active bank and its six controls. |
| `PUT/PATCH YAML FILTER SCHEMA 7` | Active tagged bank only. |
| `GET/PUT/PATCH YAML SETTINGS SCHEMA 7` | Shared settings and dialect selection. |
| `PUT YAML CONFIG SCHEMA 7` | Complete both-bank restore. |
| `VALIDATE YAML CONFIG SCHEMA 7` | Complete both-bank validation, without applying/saving. |
| `PATCH YAML CONFIG SCHEMA 7` | Ordinary active-bank edits and shared settings. A filter edit cannot also supply dialect selection. |

Upload bodies require `schema_version: 7`, `request_id`, `configuration`, and
`if_revision` for PUT/PATCH. The explicit header and body versions must agree.
Refetch after a stale revision. One revision covers both banks, their controls,
shared preferences and selected dialect; switching away and back does not
revive an earlier token. Nulls, unknown/read-only fields, aliases, merges,
duplicate keys and extra documents are refused.

In CC FILTER, `configuration` contains `dialect: cc` and `filters` with `modes`,
`sources`, `geography`, `dxbm` and the six named controls. Geography owns DOC,
DOS, DXCTY and DXSTATE; each has `global` and `bands`. Category rows use `action`
and the applicable `countries` or `states` list. Band keys are native CC names.

PATCH mode/source lists replace the list; named controls update individually.
An empty mode or source list allows nothing in that dimension. An empty
geography PASS list matches nothing; an empty REJECT list blocks nothing.
Use OFF to remove a geography restriction.
Band-only PATCH edits named rows, with `{bands: {}}` a no-op. Patch-only
`action: INHERIT` removes a band override and accepts no values. Global plus
bands for the same category in one PATCH is refused; different categories can
be edited together. DXBM PATCH replaces its complete `active`, `base_allow`
and `slots` state. OFF is explicit; absence means preserve/inherit as applicable.

Schema six keeps its 65,536-byte headers/body contract. FILTER and CONFIG are
available only while GO is effective; CONFIG proposals must remain in GO.
SETTINGS and CAPABILITIES remain available in CC with compatibility guidance,
without showing an empty or inactive GO filter as CC state. SETTINGS may switch
banks. Schema six is not a complete backup and cannot edit the inactive bank.

## Presets, limits and recovery

SAVE stores the active bank, its controls and shared preferences with a GO/CC
tag. All numeric SSIDs share 20 names. LOAD restores that tagged bank and shared
preferences, announces any dialect switch, and leaves the other bank intact.
Each bank keeps its own association and immutable applied baseline. Deleting or
overwriting a library name does not change an already applied baseline.

Machine writes and LOAD commit before changing live state. Human edits remain
active after a late save failure and carry an unsaved warning; both banks survive
switching until a complete save succeeds. SAVE can report library success with
profile-association failure; both previous associations are retained.

Native CC state is limited to 128 KiB, full CONFIG/tagged presets to 192 KiB,
and complete schema-seven frames/native human filter output to 256 KiB. Profile
files remain limited to 1 MiB; libraries to 8 MiB/20 names. Complete encoding,
metadata, both applied baselines and decode-work limits also apply. Independently
maximal lists need not fit together. Replacing lists must fit one command line
or one YAML request; splitting them into replacing commands changes their meaning.

Upgrade backups use `.pre-v7.bak`; a different existing backup is never
overwritten. Profile reset, shared-library reset and notice acknowledgement have
separate durable completion. A successful full notice flush establishes a
transmission opportunity, not proof that you read it. Failed acknowledgement
can repeat the historical notice, never the reset.

Stop writers before backups or restoration. Only one process may write a user
directory. Rollback requires a matching old binary, profiles and shared library;
preserve current files first. Restoring pre-upgrade backups loses later changes
across affected SSIDs. Do not relabel version fields to force an older reader.

See [ADR-0330](decisions/ADR-0330-native-cc-banks-and-generation-seven.md) and
the [approved contract](cc-dialect-v4-scope-ledger.md). CC User compatibility
remains an explicitly unverified follow-up.
