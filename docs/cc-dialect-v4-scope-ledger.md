# CC dialect compatibility: approved scope v4

Status: approved by the owner's exact `Approved v4` instruction on 2026-10-29.
Baseline: `boolean`, `7373e831b943a8e1ec3567687da80d6aec7f5dfa`.

This record captures the conversation's approved ledger and its final backup
clarification. Approval covers code, fixtures, validation and documentation;
deployment, production data operations and service restarts are excluded.

## User contract

- Keep independent GO and native CC filter banks and six controls in each bank.
  Switching activates the selected bank without rewriting the other.
- On first upgrade login, discard old profile filter definitions, initialize both
  banks from the same effective operator mode/source defaults, reset BEACON,
  SELF, TOXIC, ANNOUNCE, WWV and WCY to defaults, and clear old applied-preset
  references/baselines. Preserve configured dialect, unrelated settings and login
  metadata under their existing restore contracts. Later logins never repeat a
  completed profile reset.
- Reset old named presets once per shared base-call library. Preserve new
  generation-seven presets across retries and later SSID upgrades. Do not retain
  old-preset adaptation for normal use. Malformed/future data remains protected.
- Track profile reset, library reset and notice acknowledgement independently.
  Retry unacknowledged notices on later logins without resetting filters. Only
  a successful complete writer flush establishes a transmission opportunity.
  It does not establish that the operator read the message.
- CC supports mode/source edits; DOC, DOS, DXCTY, DXSTATE global PASS/REJECT/OFF;
  documented implied-DOC shorthand; DXBM named bands/slots; separate controls;
  inspection; and SET/NOFILTER. Keep configured shortcut ownership and identify
  GoCluster extensions. Boolean rule mutations are refused in CC.
- Mode selection applies to HUMAN and SKIMMER. Source edits do not change modes
  or activate feeds. Geography values use OR; enabled categories use AND.
  Geography edits replace the named mode/list. Global edits clear that category's
  band exceptions even if the supplied global value is unchanged. Band-only
  edits preserve other bands/categories. Missing/invalid metadata fails PASS
  and survives REJECT, using existing field readers without new nationality rules.
- Per-band geography is available through GoCluster YAML; native terminal
  syntax and CC User protocol integration remain unverified and deferred.
- CC's 15 native frequency bands/37 slots use Spot.Frequency, not cached GO band
  labels or ObservedFrequency. DXBM OFF is unrestricted; first PASS after OFF
  seeds a whitelist, first REJECT a blacklist, and later named edits preserve
  other slots. Whole-band edits affect all its slots. The upper slot owns shared
  endpoints; the final upper band edge is included. Decoded-mode filtering also
  applies. Preserve OFF versus active allow-all as different future-edit states.
- SET/NOFILTER restores current operator mode/source defaults and removes CC
  geography, band exceptions and DXBM restrictions, preserving its six switches,
  GO state and shared preferences. This differs from the one-time upgrade wipe.
- Preserve existing own-call live/history exceptions and TOXIC behavior; explain
  the existing GoCluster SELF/native SELF naming difference. OWN, BOB, DX/NODX,
  TALK controls, announcement/weather geography and independent WX are deferred.
- Retain human automatic read pauses and machine GET behavior. Explain that
  missed spots are not replayed and RESUME resumes delivery. Preserve admission-
  time matching; an acknowledgement is not a strict barrier for queued spots.

## State, YAML and compatibility

Use a single atomic version-seven profile holding both canonical banks, shared
settings, selection, and up to two applied references. Prepare immutable CC
category composition, sharing appropriate field readers with GO; do not expand
CC into the public GO expression/slot budgets. Matching does no parsing, CTY
lookup, configuration copying or persistence. Country storage is sparse.

Schema 7 FILTER edits only the tagged active bank; SETTINGS owns shared settings
and selection; full CONFIG restores both banks/controls/settings/selection.
Read-only metadata, baselines and the preset library are not writable CONFIG.
Ordinary edits cannot alter inactive-bank fields or combine filter editing with
selection. Complete CONFIG restore can carry inactive GO Boolean content in CC.

PATCH replaces supplied mode/source lists and named controls. A supplied global
geography row clears that category's overrides; a bands-only edit changes named
rows; an empty bands mapping changes none. Explicit band OFF is unrestricted;
patch-only INHERIT removes exactly one override. Reject global and bands in the
same category PATCH, null, unknown fields, wrong types and contradictory values.
Full PUT describes the complete desired state. DXBM PATCH supplies complete
active/base/slot state rather than relying on omitted-value defaults.

Schema-six compatibility uses explicit effective-dialect guards under the lease:

| Operation | GO active | CC active |
| --- | --- | --- |
| GET FILTER/CONFIG | GO legacy view | Refuse; use schema 7 |
| PUT/PATCH FILTER | Change GO rules/controls only | Refuse |
| PUT/PATCH CONFIG, VALIDATE CONFIG | Require resulting effective GO | Refuse |
| GET/PUT/PATCH SETTINGS | Supported, including selection | Supported, including selection |
| GET CAPABILITIES | Supported | Supported with limitations |

All views use one preference revision covering both banks/controls/shared values
and selection. Notification/login metadata does not affect this revision or
history. In CC, legacy settings/capabilities must not fabricate Boolean rules,
expose inactive GO as active, or convert native baselines to v6. Schema 6 is not
a complete backup. Preserve its legacy upload headers and 65,536-byte limit.
New uploads use `PUT|PATCH|VALIDATE YAML <resource> SCHEMA 7`, cross-check body
version, and retain terminal handling of malformed/unconsumed upload tails.

## Persistence and failure boundaries

During the one-time upgrade reset, admit each replacement and verify the
applicable original backup before replacing that file. Ordinary saves and notice
acknowledgements retain their established atomic persistence contracts; they do
not create upgrade backups.

When both upgrade replacements are eligible, admit/verify both first, then replace
library followed by profile. They are not a two-file transaction. A library failure
may leave preset operations unavailable while an independently backed-up profile
reset completes, with accurate partial-result notices. Never automatically undo a
completed library reset after profile failure. Retry unfinished work from current
authoritative files under existing SSID/base-call ownership.

Unsafe read, recognition, preparation, backup or replacement failures protect the
original. Protected temporary sessions cannot overwrite it on cleanup. Only the
initial authoritative missing-file result permits creation. Do not compile old
profile/preset rule semantics merely to discard recognized old fields.

Human late-save failures retain active unsaved state in both banks, including
after switching, with warnings until a complete save succeeds. Machine writes
and LOAD commit before publication. SAVE retains its named-library-success/
association-failure partial result and previous association.

Pending notices use fixed server-owned profile/library event and acknowledgement
metadata, a bounded flush receipt independent of PAUSE epochs, and the existing
connection owner for metadata persistence. No writer disk I/O or new worker.
Recheck current ownership and reload the current profile; acknowledge only the
captured events without invoking reset/creation or overwriting intervening edits.

## Bounds and evidence

Retain physical profile 1 MiB, preset 256 KiB, library 8 MiB/20-entry limits and
whole-document decoding-work budgets. Enforce provisional hard ceilings of CC
canonical state 128 KiB, complete CONFIG/tagged preset 192 KiB, final schema-seven
frame 256 KiB. Actual encodings, both baselines, metadata, human inspection and
complete export/restore frames must fit before accepting changes. Independently
maximal lists need not fit simultaneously. Raising ceilings or weakening promised
capacity requires revised scope. Validate operator defaults before activation.

Preserve ordinary command_line_limit; do not split replacing lists. Retain fixed
process input/output reservations, one large output per client, ownership through
flush/discard, lifecycle-fenced enqueue, one composed queue drain, buffer release,
and bounded diagnostics. Bounds describe payload/work, not exact allocator heap.

Implementation checkpoints: (1) pure native model/codecs/capacity fixtures;
(2) bounded transport/header/lifecycle; (3) integrated reset, notices, presets,
publication/history, terminal/YAML interfaces and docs. First two must remain
safe without public activation or version-seven profile writes.

Run full native Windows ordinary/race validation, vet, Staticcheck, golangci-lint,
targeted fault/lifecycle/parser tests, fuzzing, zero-allocation matching benchmarks
and local runtime profiles. Independently transcribe expected frequency vectors.
Test actual positive spot delivery after RESUME. The decisive reset test interrupts
every persistence boundary, retries from another SSID, creates a new preset when
permitted, retries pending profile/notice work, and proves the new preset survives
and old presets do not return. Library completion marker and reset replacement
must become durable together.

Rollback is a separately authorized operation: stop writers, preserve the current
dataset, restore a compatible pre-upgrade profile/library set with the matching
binary. Restoration loses later changes, including every SSID's later shared
preset edits. Keep protected originals; never edit version markers to downgrade.
