# ADR-0356: Fixed Meta Bands And Shared Physical Catalogue

- Status: Accepted
- Date: 2026-11-17
- Decision Origin: Design; exact Approved v1

## Context

Users need convenient names for common collections of bands without learning
another filter model. The owner selected six fixed meta bands, general 4m
recognition and a shared physical catalogue for their ranges. The owner clarified
that command processing selects member bands; it does not define frequencies.
Existing native CC labels, slots and saved profiles retain their meanings.

GO already represents BAND lists. Native CC's 37 slots cannot express exact UHF
coverage: its broad MW slot starts above 70cm and includes frequencies outside
the supported physical bands. Reusing MW for UHF would silently select the wrong
frequencies. Archive rows may also retain a nonempty unknown-band label from
before 4m was recognized.

## Decision

Keep numeric physical ranges in `spot/bands.go` and immutable meta memberships
as physical band identifiers in `spot/meta_bands.go`. The six names are WARC,
HFCONTEST, HF, VHF, UHF and VHFUP; the
[domain contract](../domain-contract.md#fixed-meta-bands) owns their exact lists.
HF deliberately includes 160m, 60m and WARC bands. Add 4m at
70.000-70.650 MHz; use 5.260-5.405 MHz for 60m and 220-225 MHz for 1.25m in
the physical catalogue. Preserve other physical ranges and the 13cm gap.

Resolve meta names during command-input preparation. GO expands BAND operands
to ordinary canonical member lists, preserving Boolean structure and existing
admission bounds. Meta names never become physical spot labels or a new filter
bank. HELP BANDS discovers the same catalogue in both command styles.

Keep legacy native CC frequency slots and geography intact. Derive a finite,
unambiguous frequency-cell partition from physical catalogue and legacy slot
endpoints. Optional `dxbm.frequency_overrides` retains exact member selections
that native slots cannot represent. Normalization compares each override with
the effective underlying slot/base decision. Later edits affect only their
covered frequencies; OFF clears native choices and overrides. Immutable
preparation keeps matching free of meta-name parsing, growing edit logs and
per-spot selection allocation.

Include overrides in clone/equality/fingerprint/count/readback, machine schema,
profiles, presets and applied baselines. Omit the optional field when unused.
Retain strict raw-count, byte and decode-work admission. Old strict readers may
refuse feature-bearing documents; no refusal may enter a reset or replacement
path. This feature performs no existing-record conversion or profile/preset wipe.

History queries containing a meta name classify candidate reported frequencies
through the physical catalogue before counts and pagination. They can find
retained 4m rows with unknown stored labels without rewriting archive data.
Existing saved-filter, self, comment and continuation rules still apply.

## Alternatives considered

1. Put numeric ranges in each command handler or meta definition: rejected
   because it creates independent range authorities.
2. Translate UHF into native MW: rejected because coverage is not equivalent.
3. Replace native CC with GO Boolean state or migrate all CC bands: rejected
   because the feature is a shared command shortcut and preserves native behavior.
4. Add a separately enabled meta filter bank: rejected because it changes how
   selections combine instead of simplifying band input.

## Consequences

### Benefits

- Users select familiar fixed collections with existing commands.
- Physical ranges have one owner; membership and HELP do not duplicate them.
- Existing CC selectors, geography, profiles and reset semantics remain intact.
- Exact meta coverage survives reconnects, presets and complete YAML round trips.

### Risks

- Overlapping native/meta edits require exact endpoint and local last-edit rules.
- Existing CC slot-only tests cannot prove the additional selection state.
- Older strict executables or clients may reject new feature-bearing documents.
- Catalogue recognition of 4m does not establish new mode inference or propagation
  model support; VOACAP and path-ingestion defaults are not extended.

### Operational impact

No deployment, service restart, archive rewrite or profile conversion is part of
implementation approval. Before using feature-bearing CC data, retain compatible
executable/profile/library backups for rollback. Restoring old backups loses
later edits; preserve current files before any restoration.

## Links

- [Approved scope and implementation evidence](../meta-bands-execution.md)
- [Fixed meta-band contract](../domain-contract.md#fixed-meta-bands)
- [Native CC commands and backups](../cc-dialect.md#fixed-meta-bands)
- [GO filtering and history](../../telnet/README.md#filters)
- [HELP source ownership](../../commands/README.md#help-source-of-truth)
- [ADR-0330 native CC ownership](ADR-0330-native-cc-banks-and-generation-seven.md)
- Supersedes: none; refines native DXBM selection capability without changing
  the legacy table or generation-seven reset contract.
- TSR: none; this is feature design rather than incident repair.
