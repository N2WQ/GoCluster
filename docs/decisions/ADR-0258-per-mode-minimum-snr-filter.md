# ADR-0258: Per-Mode Minimum SNR Filter

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Design

## Context

Users need to suppress weak automated reports independently by mode through
the existing PASS/REJECT commands. Reports already have an integer value and
presence flag, and live delivery and archive history share the filter matcher.
The user selected per-mode minima, human/missing-report exemptions, and retention
of removed-mode thresholds as inactive until the exact canonical mode returns.
Scope Ledger v1 received exact authorization before implementation.

## Decision

1. Both numeric `PASS MINSNR <modes|ALL> <dB>` and `REJECT MINSNR` set the same
   inclusive minimum: an automated spot with a present report passes this
   category when `Report >= minimum`. Zero and negative signed integers enable
   thresholds; absence of a map key disables it. Other filters still apply.
   Existing live/history self-spot exceptions remain unchanged.
2. Mode lists are comma or space separated and validated completely before
   publication. Numeric updates affect selected supported canonical modes only.
   ALL must stand alone and expands the current supported modes. PASS's value
   ALL and REJECT's value NONE clear selected thresholds. Global clears and
   existing whole-filter resets remove active and dormant entries.
3. Store exact mode keys and signed values in one owned `min_snr` map. Removed
   or newly aliased keys stay inactive without being rebound. Human clear
   commands resolve retained exact names before alias normalization. Supplied
   machine maps may retain a dormant key only when the transaction's prior
   configuration already contains it. Unsupported new keys are rejected.
4. Bound every map to 128 total entries and 65,536 aggregate raw ASCII key
   bytes before typed decoding or detached copying. Uppercase letters, digits,
   hyphens and underscores form the nonempty stored-name grammar. Readbacks
   expose every retained entry, configured/inactive counts and exemptions.
   Existing final response and preset encoding budgets remain independent.
5. Write disk configuration version 3. Legacy and versions 1/2 acquire no
   minima, with existing state migration pinned to its introduction at version
   2. Preserve exact old values and nested applied-preset baselines. Reject new
   fields in older formats, including actual legacy merge sources. Future or
   malformed records keep their existing protected-record behavior.
6. Expose `min_snr` only through explicit machine schema 3. Freeze versions
   1/2's field order, status and choice-object shapes. Older writes preserve
   hidden thresholds; revisions cover the complete configuration. Full schema
   3 PUT/VALIDATE requires the map; PATCH omission preserves it and a supplied
   map replaces it completely, including `{}` to clear it.
7. Include minima in configuration conversion, cloning, comparison, revision
   fingerprints, preset modification status and detached history snapshots.
   A threshold edit invalidates history continuation and pending publication.
   Use existing locks and transaction ownership, without another cache,
   registry, synchronization primitive or background worker.

## Alternatives considered

1. General numeric expressions: broader than the selected minimum-only command
   contract and would give REJECT different numeric behavior.
2. A telnet-only setting: would split ownership from shared matching,
   persistence, presets and history snapshots.
3. Expanding YAML schema 2 in place: would break strict existing clients.
4. Rejecting a whole profile after removing a mode: the user selected dormant
   retention and exact-mode reactivation instead.

## Consequences

### Benefits

- Weak automated reports can be filtered independently by mode.
- Signed zero/negative settings and exempt human/missing reports are explicit.
- Saved rules, presets and history maintain one coherent configuration.

### Risks

- Numeric PASS and REJECT are equivalent specifically for MINSNR; HELP and
  readbacks must make this exception clear.
- Dormant rules consume the same finite limits as active ones. A command that
  would exceed either bound fails without partial changes.
- Older binaries cannot read disk version 3 safely. Downgrade uses matching
  profile/preset backups rather than editing only the version marker.

### Operational impact

- Default minima are disabled. Source parsing, aggregation, report units,
  archive layout and model behavior do not change.
- Valid human edits retain existing live-first save/log behavior. Machine
  validation or persistence failure preserves live and saved state atomically.
- Human readbacks retain their width, byte and delivery-pause contracts.

## Links

- Authorization: user `Approved v1` in the implementation conversation.
- Implementation: `filter/min_snr.go`, `filter/min_snr_storage.go`,
  `telnet/minsnr_commands.go`, `telnet/machine_versions.go`.
- Tests: `filter/min_snr_test.go`, `telnet/minsnr_commands_test.go`,
  `telnet/minsnr_machine_test.go`, `telnet/minsnr_history_test.go`,
  `telnet/minsnr_readback_test.go`.
- Validation and scope mapping: [local evidence](../minsnr-validation.md).
- Docs: [telnet contract](../../telnet/README.md#minimum-snr),
  [operator guide](../OPERATOR_GUIDE.md#per-mode-minimum-snr),
  [support guidance](../../customgpt/support-cards/configuration-readback.md).
- Related TSRs: none; this is a feature design decision.
- Refines [ADR-0244](ADR-0244-exact-configuration-persistence.md),
  [ADR-0245](ADR-0245-machine-yaml-configuration.md) and
  [ADR-0253](ADR-0253-fcc-state-enrichment-and-filtering.md) for disk/schema
  version 3; other accepted contracts remain in force.
